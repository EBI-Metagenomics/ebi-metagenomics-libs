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

from mgnify_backlog import mgnify_handler
from django.core.exceptions import ObjectDoesNotExist

import logging

logging.basicConfig(level=logging.INFO)


def parse_args(args):
    parser = argparse.ArgumentParser(description='Tool to store analysis requests in the EMG backlog database')
    parser.add_argument('study', help='Study accession (all are supported)')
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--private', action='store_true')
    parser.add_argument('--failed_runs', )
    return parser.parse_args(args)


def main(argv=None):
    args = parse_args(argv)
    mh = mgnify_handler.MgnifyHandler('dev' if args.dev else 'prod')
    try:
        user = mh.get_user(args.webinID)
    except ObjectDoesNotExist:
        logging.warning('User {} not in db, creating user.'.format(args.webinID))
        user = mh.create_user(webin_id=args.webinID, email='unknown@unknown.com', first_name='first', surname='last',
                              registered=True, consent_given=True)
        logging.info('Created user {}'.format(args.webinID))

    try:
        request = mh.get_user_request(args.RTticket)
        logging.warning('Request already exists according to RTticket number')
    except ObjectDoesNotExist:
        request = mh.create_user_request(user, args.priority, args.RTticket)

    if 'MGYS' in args.study:
        study_accession = get_study(args.webinID, args.study)
    else:
        study_accession = args.study

    try:
        study = mh.get_backlog_study(study_accession)
    except ObjectDoesNotExist:
        study = mh.create_study_obj(study_data)
        logging.info('Created study {}'.format(study_accession))

    annotated_runs = mh.get_up_to_date_annotation_jobs(study_accession)
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
