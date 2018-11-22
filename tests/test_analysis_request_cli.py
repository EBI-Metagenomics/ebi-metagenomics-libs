import analysis_request_cli.create_request as creq

import pytest

from mgnify_backlog import mgnify_handler
from backlog.models import Study, Run, RunAssembly, AssemblyJob, Assembler, AssemblyJobStatus, RunAssemblyJob, \
    User, Pipeline, UserRequest, Assembly, AnnotationJob

from tests.util import clean_db


# class TestRequestCLI(object):
    # def setup_method(self, method):
    #     clean_db()
    #
    # def taredown_method(self, method):
    #     clean_db()
    #
    # def test_get_secondary_accession__should_retrieve_secondary_accession_of_study(self):
    #     assert creq.get_study_secondary_accession('Webin-460', 'MGYS00000410') == 'ERP001736'
    #
    # def test_get_secondary_accession_should_raise_exception_on_invalid_mgys_accession(self):
    #     with pytest.raises(ValueError):
    #         creq.get_study_secondary_accession('Webin-460', 'MGYS_INVALID')
    #
    # def test_get_user_details_should_retrieve_user_details(self):
    #     assert creq.get_user_details('Webin-460')['email-address'] == 'metagenomics@ebi.ac.uk'
    #
    # def test_get_user_details_should_raise_exception_on_invalid_webin_account(self):
    #     with pytest.raises(ValueError):
    #         creq.get_user_details('Webin_INVALID')

    # def test_main_should_create_full_request(self):
    #     secondary_accession = 'DRP000303'
    #     creq.main([secondary_accession, 'Webin-460', '1', '--db', 'default', '--lineage', 'root:Host-Associated:Human'])
    #     studies = Study.objects.all()
    #     assert len(studies) == 1
    #     assert studies[0].secondary_accession == secondary_accession
    #     runs = Run.objects.all()
    #     assert len(runs) == 2
    #     assert [run.study.secondary_accession == secondary_accession for run in runs]
