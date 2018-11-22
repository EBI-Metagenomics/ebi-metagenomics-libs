import analysis_request_cli.create_request as creq

import pytest

from mgnify_backlog import mgnify_handler
from backlog.models import Study, Run, RunAssembly, AssemblyJob, Assembler, AssemblyJobStatus, RunAssemblyJob, \
    User, Pipeline, UserRequest, Assembly, AnnotationJob

from tests.util import clean_db, study_data


class TestRequestCLI(object):
    def setup_method(self, method):
        clean_db()

    def taredown_method(self, method):
        clean_db()

    def test_get_secondary_accession__should_retrieve_secondary_accession_of_study(self):
        assert creq.get_study_secondary_accession('Webin-460', 'MGYS00000410') == 'ERP001736'

    def test_get_secondary_accession_should_raise_exception_on_invalid_mgys_accession(self):
        with pytest.raises(ValueError):
            creq.get_study_secondary_accession('Webin-460', 'MGYS_INVALID')

    def test_get_user_details_should_retrieve_user_details(self):
        assert creq.get_user_details('Webin-460')['email-address'] == 'metagenomics@ebi.ac.uk'

    def test_get_user_details_should_raise_exception_on_invalid_webin_account(self):
        with pytest.raises(ValueError):
            creq.get_user_details('Webin-0000')

    def test_main_should_create_full_request(self):
        Pipeline(version=4.1).save()
        secondary_accession = 'SRP077065'
        webin_id = 'Webin-460'
        rt_ticket = 1
        creq.main(
            [secondary_accession, webin_id, str(rt_ticket), '--annotate', '--db', 'default', '--lineage',
             'root:Host-Associated:Human'])

        studies = Study.objects.all()
        assert len(studies) == 1
        assert studies[0].secondary_accession == secondary_accession
        runs = Run.objects.all()
        # Check runs were inserted and linked to correct study
        assert len(runs) == 2
        for run in runs:
            assert run.study.secondary_accession == secondary_accession
        run_accessions = [run.primary_accession for run in runs]
        # Check annotationJobs were inserted and linked to correct pipeline and run
        annotation_jobs = AnnotationJob.objects.all()
        assert len(annotation_jobs) == 2
        for job in annotation_jobs:
            assert job.pipeline.version == 4.1
        for job in annotation_jobs:
            assert job.pipeline.version == 4.1
            assert job.runannotationjob_set.first().run.primary_accession in run_accessions

        requests = UserRequest.objects.all()
        assert len(requests) == 1
        assert requests[0].priority == 0
        assert requests[0].rt_ticket == rt_ticket
        assert requests[0].user.webin_id == webin_id

    def test_main_should_not_create_duplicate_annotation_request(self):
        Pipeline(version=4.1).save()
        secondary_accession = 'SRP077065'
        creq.main([secondary_accession, 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage', 'root:Host-Associated:Human'])
        with pytest.raises(SystemExit):
            creq.main(
                [secondary_accession, 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage', 'root:Host-Associated:Human'])
        # Check runs were inserted and linked to correct study
        assert len(Run.objects.all()) == 2
        assert len(AnnotationJob.objects.all()) == 2
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_create_annotation_request_with_mgys_accession(self):
        Pipeline(version=4.1).save()
        mgys_accession = 'MGYS00003133'
        creq.main([mgys_accession, 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage', 'root:Host-Associated:Human'])
        # Check runs were inserted and linked to correct study
        assert len(Run.objects.all()) == 14
        assert len(AnnotationJob.objects.all()) == 14
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_create_annotation_request_with_primary_accession(self):
        Pipeline(version=4.1).save()
        primary_accession = 'PRJNA262656'
        creq.main([primary_accession, 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage', 'root:Host-Associated:Human'])
        # Check runs were inserted and linked to correct study
        assert len(Run.objects.all()) == 14
        assert len(AnnotationJob.objects.all()) == 14
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_raise_exception_if_mgys_accession_is_invalid(self):
        with pytest.raises(ValueError) as e:
            creq.main(['MGYS_INVALID', 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage', 'root:Host-Associated:Human'])

    def test_main_should_require_lineage_to_insert_run(self):
        with pytest.raises(ValueError) as e:
            creq.main(['SRP077065', 'Webin-460', '1', '--annotate', '--db', 'default'])

    def test_main_should_create_assembly_job(self):
        Assembler(name='metaspades', version='3.12.0').save()
        AssemblyJobStatus(description='pending').save()
        creq.main(['SRP077065', 'Webin-460', '0', '--assemble', '--lineage', 'root'])
        assert len(Run.objects.all()) == 2
        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 2
        for job in assembly_jobs:
            assert job.assembler.name == 'metaspades'
            assert job.assembler.version == '3.12.0'
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_throw_error_if_both_annotate_and_assemble_flags_not_given(self):
        with pytest.raises(SystemExit):
            creq.main(['SRP077065', 'Webin-460', '0', '--lineage', 'root'])
