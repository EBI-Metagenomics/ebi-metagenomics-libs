import pytest
import os
from datetime import datetime

from mgnify_backlog import mgnify_handler
from ena_portal_api import ena_handler

from unittest.mock import MagicMock

from django.core.exceptions import ObjectDoesNotExist

from backlog.models import Study, Run, AssemblyJob, Assembler, AssemblyJobStatus, RunAssemblyJob, Biome, User, Pipeline, \
    UserRequest, AnnotationJobStatus, Assembly, AnnotationJob, AssemblyAnnotationJob, RunAnnotationJob


class MockResponse:
    def __init__(self, status_code, data=None, text=None):
        self.data = data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.data


study_data = {
    'study_accession': 'PRJEB12345_test',
    'secondary_study_accession': 'ERP0012345_test',
    'study_title': 'Study title',
    'first_public': '2018-05-05',
    'last_updated': '2018-05-05'
}

run_data = {
    'run_accession': 'ERR12345_test',
    'base_count': 1,
    'read_count': 2,
    'instrument_platform': 'ILLUMINA',
    'instrument_model': 'MiSeq 2500',
    'library_strategy': 'WGS',
    'library_layout': 'PAIRED',
    'library_source': 'METAGENOMIC',
    'last_updated': '2018-05-05',
    'lineage': 'root:Environmental:Aquatic:Marine'
}


class TestBacklogHandler(object):
    def setup_method(self, method):
        # Clean db of study prior to test
        try:
            Study.objects.using('default').get(primary_accession=study_data['study_accession']).delete()
        except Exception:
            pass
        try:
            Run.objects.using('default').get(primary_accession=run_data['run_accession']).delete()
        except Exception:
            pass

    def test_save_and_retrieve_study(self):
        mgnify = mgnify_handler.MgnifyHandler('default')
        mgnify.create_study_obj(study_data)
        study = mgnify.get_backlog_study(study_data['study_accession'])

        assert type(study) == Study
        assert study.primary_accession == study_data['study_accession']
        assert study.secondary_accession == study_data['secondary_study_accession']
        assert study.title == study_data['study_title']
        assert study.public
        assert study.ena_last_update == datetime.strptime(study_data['last_updated'], "%Y-%m-%d").date()

        study.delete()

    def test_get_study_by_secondary_accession(self):
        mgnify = mgnify_handler.MgnifyHandler('default')
        mgnify.create_study_obj(study_data)
        study = mgnify.get_backlog_secondary_study(study_data['secondary_study_accession'])

        assert type(study) == Study
        assert study.primary_accession == study_data['study_accession']
        assert study.secondary_accession == study_data['secondary_study_accession']
        assert study.title == study_data['study_title']
        assert study.public
        assert study.ena_last_update == datetime.strptime(study_data['last_updated'], "%Y-%m-%d").date()

        study.delete()

    def test_save_and_retrieve_run(self):
        mgnify = mgnify_handler.MgnifyHandler('default')
        study = mgnify.create_study_obj(study_data)
        mgnify.create_run_obj(study, run_data)
        run = mgnify.get_backlog_run(run_data['run_accession'])

        assert type(run) == Run
        assert run.primary_accession == run_data['run_accession']
        assert run.base_count == run_data['base_count']
        assert run.read_count == run_data['read_count']
        assert run.instrument_platform == run_data['instrument_platform']
        assert run.instrument_model == run_data['instrument_model']
        assert run.library_strategy == run_data['library_strategy']
        assert run.library_layout == run_data['library_layout']
        assert run.library_source == run_data['library_source']
        assert run.ena_last_update == datetime.strptime(run_data['last_updated'], "%Y-%m-%d").date()

        run.delete()
        study.delete()

    def test_get_or_save_study_should_find_existing_study(self):
        mgnify = mgnify_handler.MgnifyHandler('default')
        inserted_study = mgnify.create_study_obj(study_data)
        retrieved_study = mgnify.get_or_save_study(None, study_data['study_accession'])

        assert type(retrieved_study) == Study
        assert retrieved_study.pk == inserted_study.pk

        inserted_study.delete()

    def test_get_or_save_study_should_fetch_from_ena(self):
        ena_study_data = {
            'study_accession': 'PRJEB1787',
            'secondary_study_accession': 'ERP001736',
            'study_title': 'Shotgun Sequencing of Tara Oceans DNA samples corresponding to size fractions for  prokaryotes.',
            'first_public': '2018-05-05',
            'last_updated': '2018-05-05'
        }
        mgnify = mgnify_handler.MgnifyHandler('default')
        ena = ena_handler.EnaApiHandler()
        retrieved_study = mgnify.get_or_save_study(ena, ena_study_data['study_accession'])

        assert type(retrieved_study) == Study
        assert len(Study.objects.all()) == 1
        assert retrieved_study.primary_accession == ena_study_data['study_accession']
        assert retrieved_study.title == ena_study_data['study_title']

        retrieved_study.delete()

    def test_get_or_save_run_should_find_existing_run(self):
        mgnify = mgnify_handler.MgnifyHandler('default')
        ena = ena_handler.EnaApiHandler()
        study = mgnify.create_study_obj(study_data)
        created_run = mgnify.create_run_obj(study, run_data)
        retrieved_run = mgnify.get_or_save_run(study, run_data, 'root')

        assert type(retrieved_run) == Run
        assert retrieved_run.pk == created_run.pk

        study.delete()
        created_run.delete()

    def test_get_or_save_run_should_fetch_from_ena(self):
        mgnify = mgnify_handler.MgnifyHandler('default')
        study = mgnify.create_study_obj(study_data)
        run = mgnify.get_or_save_run(study, run_data, 'root')

        assert type(run) == Run
        assert run.primary_accession == run_data['run_accession']
        assert run.base_count == run_data['base_count']
        assert run.read_count == run_data['read_count']
        assert run.instrument_platform == run_data['instrument_platform']
        assert run.instrument_model == run_data['instrument_model']
        assert run.library_strategy == run_data['library_strategy']
        assert run.library_layout == run_data['library_layout']
        assert run.library_source == run_data['library_source']
        assert run.ena_last_update == datetime.strptime(run_data['last_updated'], "%Y-%m-%d").date()

        study.delete()
        run.delete()

    def test_get_or_save_run_should_require_lineage_to_insert_run(self):
        mgnify = mgnify_handler.MgnifyHandler('default')
        study = mgnify.create_study_obj(study_data)
        with pytest.raises(ValueError):
            mgnify.get_or_save_run(study, run_data, None)
