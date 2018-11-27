#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2018 EMBL - European Bioinformatics Institute
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
import time

logging.basicConfig(level=logging.WARN)

API_DATA_URL = os.environ.get('MGNIFY_API_URL', 'https://www.ebi.ac.uk/metagenomics/api/v1/')

API_PASSWORD = os.environ['MGNIFY_API_PASSWORD']

LOGIN_URL = os.environ.get('MGNIFY_API_LOGIN_URL', 'https://www.ebi.ac.uk/metagenomics/api/http-auth/login/')
LOGIN_FORM = os.environ.get('MGNIFY_API_LOGIN_FORM', 'https://www.ebi.ac.uk/metagenomics/api/http-auth/login_form')

MAX_RETRIES = 5


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Tool to store analysis & assembly requests in the EMG backlog database')
    parser.add_argument('study', help='Study accession (all are supported)')
    parser.add_argument('webinID', help='Webin id')
    parser.add_argument('RTticket', help='RT ticket')
    parser.add_argument('--db', choices=['default', 'dev', 'prod'], default='default')
    parser.add_argument('--private', action='store_true')
    parser.add_argument('--priority', type=int, choices=range(0, 5), default=0)
    parser.add_argument('--lineage', help='Full lineage of biome')
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--assemble', action='store_true', help='Create assemblyJobs')
    actions.add_argument('--annotate', action='store_true', help='Create annotationjobs')
    return parser.parse_args(args)


def authenticate_session(session, webin_id):
    _ = session.get(LOGIN_FORM).headers
    csrftoken = session.cookies['csrftoken']

    session.headers.update({'referer': 'https://www.ebi.ac.uk'})
    login_data = {'username': webin_id, 'password': API_PASSWORD, 'csrfmiddlewaretoken': csrftoken}
    _ = session.post(LOGIN_URL, data=login_data)
    return session


def get_user_details(webin_id):
    with requests.Session() as s:
        try_count = 1
        while True:
            s = authenticate_session(s, webin_id)
            req = s.get(os.path.join(API_DATA_URL, 'utils', 'myaccounts'), auth=HTTPBasicAuth(webin_id, API_PASSWORD))
            user = json.loads(req.text)
            try:
                return user['data'][0]['attributes']
            except KeyError:
                if user['errors'][0]['status'] == '401' and try_count <= MAX_RETRIES:
                    time.sleep(1)
                    logging.warning('Could not fetch user details, retrying ({}/{})'.format(try_count, MAX_RETRIES))
                    try_count += 1
                    continue
                logging.error(user)
                raise ValueError('Could not retrieve user details.')


# Get secondary accession of study from MGnify API
def get_study_secondary_accession(webin_id, mgys):
    with requests.Session() as s:
        s = authenticate_session(s, webin_id)

        req = s.get(os.path.join(API_DATA_URL, 'studies', mgys), auth=HTTPBasicAuth(webin_id, API_PASSWORD))
        study = json.loads(req.text)
        try:
            return study['data']['attributes']['secondary-accession']
        except KeyError:
            logging.error(study)
            if study['errors'][0]['status'] == '404':
                msg = 'Study {} does not exist in db'.format(mgys)
            else:
                msg = 'API response to study query was not valid (try again)'
            raise ValueError(msg)


# Remove runs from set if they were already analysed with latest pipeline
def filter_duplicate_runs(annotated_runs, run_accessions):
    annotated_run_accession = [run.primary_accession for run in annotated_runs]
    return list(filter(lambda r: r not in annotated_run_accession, run_accessions))


def create_new_run_jobs(ena, mh, study, request, args):
    runs = ena.get_study_run_accessions(study.secondary_accession, False, args.private)
    if not len(runs):
        logging.warning('No runs to annotate in this study.')
        return
    annotated_runs = mh.get_up_to_date_run_annotation_jobs(study.secondary_accession)
    runs = filter_duplicate_runs(annotated_runs, runs)
    if not len(runs):
        logging.warning('All runs in this study are annotated with the latest pipeline.')
        return

    for run in runs:
        run = mh.get_or_save_run(ena, study, run, args.lineage)
        if args.annotate:
            mh.create_annotation_job(request, run, args.priority)
            msg = 'Created annotationJob for run {}'
        else:
            mh.create_assembly_job(run, run.compressed_data_size, 'pending', 'metaspades')
            msg = 'Created assemblyJob for run {}'
        logging.info(msg.format(run.primary_accession))


def create_new_assembly_annotation_jobs(ena, mh, study, request, args):
    assemblies = ena.get_study_assembly_accessions(study.primary_accession)
    if not len(assemblies):
        logging.warning('No assemblies to annotate in this study.')
        return
    annotated_assemblies = mh.get_up_to_date_assembly_annotation_jobs(study.secondary_accession)
    assemblies = filter_duplicate_runs(annotated_assemblies, assemblies)
    if not len(assemblies):
        logging.warning('All assemblies in this study are annotated with the latest pipeline.')
        return

    for assembly in assemblies:
        assembly = mh.get_or_save_assembly(ena, study, assembly, args.lineage)
        mh.create_annotation_job(request, assembly, args.priority)
        logging.info('Created annotationJob for assembly {}'.format(assembly.primary_accession))


def main(argv=None):
    args = parse_args(argv)
    if not args.annotate and not args.assemble:
        logging.error('No job type specified, please set --annotate or --assemble in arugments.')
        sys.exit(1)
    mh = mgnify_handler.MgnifyHandler(args.db)
    ena = ena_handler.EnaApiHandler()
    try:
        user = mh.get_user(args.webinID)
    except ObjectDoesNotExist:
        logging.warning('User {} not in db, creating user.'.format(args.webinID))
        user_details = get_user_details(args.webinID)
        user = mh.create_user(webin_id=args.webinID, email=user_details['email-address'],
                              first_name=user_details['first-name'], surname=user_details['surname'],
                              registered=True, consent_given=True)
        logging.info('Created user {}'.format(args.webinID))

    try:
        request = mh.get_user_request(args.RTticket)
        logging.warning('Request already exists according to RTticket number')
    except ObjectDoesNotExist:
        request = mh.create_user_request(user, args.priority, args.RTticket)

    # Handle MGnify accessions
    if 'MGYS' in args.study:
        accession = get_study_secondary_accession(args.webinID, args.study)
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
        except ValueError:
            raise ('Could not get study {} from ena.'.format(accession))
        logging.info('Created study {}'.format(accession))

    logging.info('Fetching study runs...')

    create_new_run_jobs(ena, mh, study, request, args)

    if not args.assemble:
        create_new_assembly_annotation_jobs(ena, mh, study, request, args)


if __name__ == '__main__':
    main(sys.argv[1:])
