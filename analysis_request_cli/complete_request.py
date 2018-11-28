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
from analysis_request_cli.create_request import get_study_secondary_accession

import logging

logging.basicConfig(level=logging.INFO)


def parse_args(args):
    parser = argparse.ArgumentParser(description='Tool to store the analysis status of study runs/assemblies in the EMG backlog database')
    parser.add_argument('study', help='Study accession (all are supported)')
    parser.add_argument('RTticket', help='RT ticket')
    parser.add_argument('--db', choices=['default', 'dev', 'prod'], default='default')
    parser.add_argument('--private', action='store_true')
    parser.add_argument('--failed_runs', nargs='+', help='Spade-separated list of run accessions which '
                                                         'could not be annotated (sets status to failed)')
    return parser.parse_args(args)


def main(argv=None):
    args = parse_args(argv)
    mh = mgnify_handler.MgnifyHandler(args.db)

    # Handle MGnify accessions
    if 'MGYS' in args.study:
        webin_id = mh.get_request_webin(args.RTticket)
        accession = get_study_secondary_accession(webin_id, args.study)
    # Handle ENA accessions
    else:
        accession = args.study

    try:
        study = mh.get_backlog_study(accession)
    except ObjectDoesNotExist:
        raise ValueError('Could not find study {} in backlog'.format(accession))

    logging.info('Setting completed jobs...')
    mh.set_annotation_jobs_completed(study, args.RTticket, args.failed_runs or [])
    if args.failed_runs:
        logging.info('Setting failed jobs...')
        mh.set_annotation_jobs_failed(study, args.RTticket, args.failed_runs)
    logging.info('Completed request successfully.')


if __name__ == '__main__':
    main(sys.argv[1:])
