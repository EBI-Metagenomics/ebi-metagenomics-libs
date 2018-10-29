from __future__ import print_function

import sys
import requests
from ruamel import yaml
import json
import os
import logging
from multiprocessing.pool import ThreadPool

ENA_API_URL = "https://www.ebi.ac.uk/ena/portal/api/search"

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

    def __init__(self, config_file=None):
        config = []
        if config_file:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)

        self.url = "https://www.ebi.ac.uk/ena/portal/api/search"
        if 'USER' in config and 'PASSWORD' in config:
            self.auth = (config['USER'], config['PASSWORD'])
        else:
            self.auth = None

    def post_request(self, data):
        if self.auth:
            response = requests.post(self.url, data=data, auth=self.auth, **get_default_connection_headers())
        else:
            response = requests.post(self.url, data=data, **get_default_connection_headers())
        return response

    def get_study(self, study_sec_acc):
        data = get_default_params()
        data['result'] = 'study'
        data['fields'] = 'study_accession,secondary_study_accession,study_description,study_name,study_title,' \
                         'center_name,broker_name,last_updated,first_public'
        data['query'] = 'secondary_study_accession=\"{}\"'.format(study_sec_acc)
        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            raise ValueError('Could not retrieve runs for study %s.', study_sec_acc)
        try:
            study = json.loads(response.text)[0]
        except IndexError:
            raise IndexError('Could not find study {} in ENA.'.format(study_sec_acc))
        return study

    def get_study_runs(self, study_sec_acc, filter_runs=True):
        data = get_default_params()
        data['result'] = 'read_run'
        data['fields'] = 'secondary_study_accession,run_accession,library_source,library_strategy,' \
                         'library_layout,fastq_ftp,base_count,read_count,instrument_platform,instrument_model',
        data['query'] = 'secondary_study_accession=\"{}\"'.format(study_sec_acc)
        response = self.post_request(data)
        if str(response.status_code)[0] != '2':
            raise ValueError('Could not retrieve runs for study %s.', study_sec_acc)

        runs = json.loads(response.text)
        if filter_runs:
            runs = list(filter(run_filter, runs))
        for run in runs:
            run['raw_data_size'] = get_run_raw_size(run)
            for int_param in ('read_count', 'base_count'):
                run[int_param] = int(run[int_param])
        return runs
        #
        # def fetch_study_runs(self, study, run_accessions):
        #     try:
        #         logging.info('Fetching runs...')
        #         runs = self.get_study_runs(study)
        #         if run_accessions:
        #             filter_runs = run_accessions.split(',')
        #             runs = list(filter(lambda x: x['run_accession'] in filter_runs, runs))
        #     except IndexError:
        #         print('No study accession specified')
        #         sys.exit(1)
        #
        #     for r in runs:
        #         r['download_job'] = []
        #         r['raw_reads'] = convert_file_locations(r['fastq_ftp'])
        #         r['read_count'] = int(r['read_count'])
        #         r['base_count'] = int(r['base_count'])
        #         del r['fastq_ftp']
        #         # TODO remove section if CWL support for ftp is fixed.
        #         for f in r['raw_reads']:
        #             dest = os.path.join(os.getcwd(), f['location'].split('/')[-1])
        #             r['download_job'].append((f['location'], dest))
        #             f['location'] = 'file://' + dest
        #
        #     return runs


def flatten(l):
    return [item for sublist in l for item in sublist]


def get_run_raw_size(run):
    urls = run['fastq_ftp'].split(';')
    return sum([int(requests.head('http://' + url).headers['content-length']) for url in urls])


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
