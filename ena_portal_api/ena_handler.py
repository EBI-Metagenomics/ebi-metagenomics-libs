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

from __future__ import print_function

import sys
import requests
from ruamel import yaml
import json
import os
import logging
from multiprocessing.pool import ThreadPool

ENA_API_URL = os.environ.get('ENA_API_URL', "https://www.ebi.ac.uk/ena/portal/api/search")

logging.basicConfig(level=logging.INFO)


def get_default_connection_headers():
    return {
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*"
        }
    }


def get_default_params():
    return {
        "dataPortal": "metagenome",
        "format": "json",
    }


def run_filter(d):
    return d['library_strategy'] != 'AMPLICON' and d['library_source'] == 'METAGENOMIC'


class EnaApiHandler:
    url = ENA_API_URL

    def __init__(self):
        self.url = "https://www.ebi.ac.uk/ena/portal/api/search"
        if 'ENA_API_USER' in os.environ and 'ENA_API_PASSWORD' in os.environ:
            self.auth = (os.environ['ENA_API_USER'], os.environ['ENA_API_PASSWORD'])
        else:
            self.auth = None

    def post_request(self, data):
        if self.auth:
            response = requests.post(self.url, data=data, auth=self.auth, **get_default_connection_headers())
        else:
            response = requests.post(self.url, data=data, **get_default_connection_headers())
        return response

    # Supports ENA primary and secondary study accessions
    def get_study(self, study_acc):
        data = get_default_params()
        data['result'] = 'study'
        data['fields'] = 'study_accession,secondary_study_accession,study_description,study_name,study_title,' \
                         'tax_id,scientific_name,center_name,last_updated,first_public,last_updated'

        if study_acc[0:3] in ('ERP', 'SRP', 'DRP'):
            data['query'] = 'secondary_study_accession=\"{}\"'.format(study_acc)
        else:
            data['query'] = 'study_accession=\"{}\"'.format(study_acc)
        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.error('Error retrieving study {}, response code: {}'.format(study_acc, response.status_code))
            logging.error('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve runs for study %s.', study_acc)
        try:
            study = json.loads(response.text)[0]
        except IndexError:
            raise IndexError('Could not find study {} in ENA.'.format(study_acc))
        return study

    def get_run(self, run_accession):
        data = get_default_params()
        data['result'] = 'read_run'
        data['fields'] = 'secondary_study_accession,run_accession,library_source,library_strategy,' \
                         'library_layout,fastq_ftp,base_count,read_count,instrument_platform,instrument_model,secondary_sample_accession',
        data['query'] = 'run_accession=\"{}\"'.format(run_accession)
        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.error('Error retrieving run {}, response code: {}'.format(run_accession, response.status_code))
            logging.error('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve runs with accession %s.', run_accession)

        runs = json.loads(response.text)
        for run in runs:
            run['raw_data_size'] = get_run_raw_size(run)
            for int_param in ('read_count', 'base_count'):
                run[int_param] = int(run[int_param])
        return runs

    def get_study_runs(self, study_sec_acc, filter_assembly_runs=True, private=False, filter_accessions=None):
        data = get_default_params()
        data['result'] = 'read_run'
        data['fields'] = 'study_accession,secondary_study_accession,run_accession,library_source,library_strategy,' \
                         'library_layout,fastq_ftp,base_count,read_count,instrument_platform,instrument_model,secondary_sample_accession',
        data['query'] = 'secondary_study_accession=\"{}\"'.format(study_sec_acc)
        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            logging.error(
                'Error retrieving study runs {}, response code: {}'.format(study_sec_acc, response.status_code))
            logging.error('Response: {}'.format(response.text))
            raise ValueError('Could not retrieve runs for study %s.', study_sec_acc)

        runs = json.loads(response.text)
        if filter_assembly_runs:
            runs = list(filter(run_filter, runs))

        if filter_accessions:
            runs = list(filter(lambda r: r['run_accession'] in filter_accessions, runs))

        for run in runs:
            if private:
                run['raw_data_size'] = None
            else:
                run['raw_data_size'] = self.get_run_raw_size(run)
            for int_param in ('read_count', 'base_count'):
                run[int_param] = int(run[int_param])
        return runs

    def get_run_raw_size(self, run):
        urls = run['fastq_ftp'].split(';')
        return sum([int(requests.head('http://' + url, auth=self.auth).headers['content-length']) for url in urls])


def flatten(l):
    return [item for sublist in l for item in sublist]


def download_runs(runs):
    urls = flatten(r['fastq_ftp'].split(';') for r in runs)
    download_jobs = [(url, os.path.basename(url)) for url in urls]
    results = ThreadPool(8).imap_unordered(fetch_url, download_jobs)

    for path in results:
        logging.info('Downloaded file: {}'.format(path))


FNULL = open(os.devnull, 'w')


def fetch_url(entry):
    uri, path = entry
    if 'ftp://' not in uri and 'http://' not in uri and 'https://' not in uri:
        uri = 'http://' + uri
    if not os.path.exists(path):
        r = requests.get(uri, stream=True)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r:
                    f.write(chunk)
    return path
