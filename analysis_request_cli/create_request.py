#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2017 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
import os
import requests
from requests.auth import HTTPBasicAuth
import json

from mgnify_backlog import mgnify_handler
from ena_portal_api import ena_handler
from django.core.exceptions import ObjectDoesNotExist

import logging

logging.basicConfig(level=logging.INFO)

API_DATA_URL = os.environ.get('MGNIFY_API_URL', 'https://www.ebi.ac.uk/metagenomics/api/v1/')

API_PASSWORD = os.environ['MGNIFY_API_PASSWORD']

LOGIN_URL = os.environ.get('MGNIFY_API_LOGIN_URL', 'https://www.ebi.ac.uk/metagenomics/api/http-auth/login/')
LOGIN_FORM = os.environ.get('MGNIFY_API_LOGIN_FORM', 'https://www.ebi.ac.uk/metagenomics/api/http-auth/login_form')


def parse_args(args):
    parser = argparse.ArgumentParser(description='Tool to store analysis requests in the EMG backlog database')
    parser.add_argument('study', help='Study accession (all are supported)')
    parser.add_argument('webinID', help='Webin id')
    parser.add_argument('RTticket', help='RT ticket')
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--private', action='store_true')
    parser.add_argument('--priority', type=int, choices=range(0, 5), default=0)
    parser.add_argument('--lineage', help='Full lineage of biome')
    return parser.parse_args(args)


def authenticate_session(session, webin_id):
    get_csrftoken = session.get(LOGIN_FORM).headers
    if 'csrftoken' in session.cookies:
        # Django 1.6 and up
        csrftoken = session.cookies['csrftoken']
    else:
        # older versions
        csrftoken = session.cookies['csrf']

    session.headers.update({'referer': 'https://www.ebi.ac.uk'})
    login_data = {'username': webin_id, 'password': API_PASSWORD, 'csrfmiddlewaretoken': csrftoken}
    login = session.post(LOGIN_URL, data=login_data)


def get_user_details(webin_id):
    with requests.Session() as s:
        authenticate_session(s, webin_id)
        req = s.get(os.path.join(API_DATA_URL, 'utils', 'myaccounts'), auth=HTTPBasicAuth(webin_id, API_PASSWORD))
        user = json.loads(req.text)
        try:
            return user['data'][0]['attributes']
        except KeyError:
            logging.error(user)
            raise EnvironmentError('API response to study query was not valid (try again)')


# Get secondary accession of study from MGnify API
def get_study_secondary_accession(webin_id, mgys):
    with requests.Session() as s:
        authenticate_session(s, webin_id)

        req = s.get(os.path.join(API_DATA_URL, 'studies', mgys), auth=HTTPBasicAuth(webin_id, API_PASSWORD))
        study = json.loads(req.text)
        try:
            return study['data']['attributes']['secondary-accession']
        except KeyError as e:
            logging.error(study)
            raise EnvironmentError('API response to study query was not valid (try again)')


# Remove runs from set if they were already analysed with latest pipeline
def filter_duplicate_runs(annotated_runs, study_runs):
    annotated_run_accession = [run.primary_accession for run in annotated_runs]
    return list(filter(lambda r: r['run_accession'] not in annotated_run_accession, study_runs))


def main(argv=None):
    args = parse_args(argv)
    mh = mgnify_handler.MgnifyHandler('dev' if args.dev else 'default')
    ena = ena_handler.EnaApiHandler()
    try:
        user = mh.get_user(args.webinID)
    except ObjectDoesNotExist:
        logging.warning('User {} not in db, creating user.'.format(args.webinID))
        user_details = get_user_details(args.webinID)
        user = mh.create_user(webin_id=args.webinID, email=user_details['email-address'], first_name=user_details['first-name'], surname=user_details['surname'],
                              registered=True, consent_given=True)
        logging.info('Created user {}'.format(args.webinID))

    try:
        request = mh.get_user_request(args.RTticket)
        logging.warning('Request already exists according to RTticket number')
    except ObjectDoesNotExist:
        request = mh.create_user_request(user, args.priority, args.RTticket)

    # Handle MGnify accessions
    if 'MGYS' in args.study:
        try:
            accession = get_study_secondary_accession(args.webinID, args.study)
        except EnvironmentError:
            raise ValueError('Study with accession {} does not exist in MGnify'.format(args.study))
    # Handle ENA accessions
    else:
        accession = args.study

    try:
        if accession[0:3] in ('ERP', 'SRP', 'DRP'):
            study = mh.get_backlog_secondary_study(accession)
        else:
            study = mh.get_backlog_study(accession)
    except ObjectDoesNotExist:
        try:
            study_data = ena.get_study(accession)
            study = mh.create_study_obj(study_data)
        except json.decoder.JSONDecodeError as e:
            raise e('Could not get study {} from ena.'.format(accession))
        logging.info('Created study {}'.format(accession))

    secondary_accession = study.secondary_accession

    runs = ena.get_study_runs(secondary_accession, False, args.private)
    annotated_runs = mh.get_up_to_date_annotation_jobs(secondary_accession)
    runs = filter_duplicate_runs(annotated_runs, runs)
    if not len(runs):
        logging.warning('No runs or assemblies left to annotate in this study.')
        sys.exit(0)

    for i, run in enumerate(runs):
        run = mh.get_or_save_run(study, run, args.lineage)
        mh.create_annotation_job(request, run, args.priority)
        logging.info('Created annotationJob for run {}'.format(run.primary_accession))


if __name__ == '__main__':
    main(sys.argv[1:])
