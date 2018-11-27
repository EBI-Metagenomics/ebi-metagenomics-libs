import pytest

import analysis_request_cli.create_request as creq
import analysis_request_cli.complete_request as ccomp

from backlog.models import Study, Run, AssemblyJob, Assembler, AssemblyJobStatus, AnnotationJobStatus, \
    Pipeline, UserRequest, AnnotationJob, Assembly

from tests.util import clean_db


class TestCreateRequestCLI(object):
    def setup_method(self, method):
        clean_db()
        Pipeline(version=4.1).save()


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

        assemblies = Assembly.objects.all()
        assert len(assemblies) == 2
        for assembly in assemblies:
            assert assembly.study.secondary_accession == secondary_accession
        assembly_accessions = [assembly.primary_accession for assembly in assemblies]

        # Check annotationJobs were inserted and linked to correct pipeline and run
        annotation_jobs = AnnotationJob.objects.all()
        assert len(annotation_jobs) == 4
        for job in annotation_jobs:
            assert job.pipeline.version == 4.1
            runs = job.runannotationjob_set.all()
            if runs:
                assert runs[0].run.primary_accession in run_accessions
            assemblies = job.assemblyannotationjob_set.all()
            if assemblies:
                assert assemblies[0].assembly.primary_accession in assembly_accessions

        requests = UserRequest.objects.all()
        assert len(requests) == 1
        assert requests[0].priority == 0
        assert requests[0].rt_ticket == rt_ticket
        assert requests[0].user.webin_id == webin_id

    def test_main_should_not_create_duplicate_annotation_request(self):
        secondary_accession = 'SRP077065'
        cmd = [secondary_accession, 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage',
               'root:Host-Associated:Human']

        creq.main(cmd)
        assert len(Assembly.objects.all()) == 2
        assert len(AnnotationJob.objects.all()) == 4
        assert len(UserRequest.objects.all()) == 1
        creq.main(cmd)
        # Check runs were inserted and linked to correct study
        assert len(Run.objects.all()) == 2
        assert len(Assembly.objects.all()) == 2
        assert len(AnnotationJob.objects.all()) == 4
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_create_annotation_request_with_mgys_accession(self):
        mgys_accession = 'MGYS00001879'
        creq.main([mgys_accession, 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage',
                   'root:Host-Associated:Human'])
        # Check runs were inserted and linked to correct study
        assert len(Run.objects.all()) == 2
        assert len(AnnotationJob.objects.all()) == 2
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_create_annotation_request_with_primary_accession(self):
        primary_accession = 'PRJNA262656'
        creq.main([primary_accession, 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage',
                   'root:Host-Associated:Human'])
        # Check runs were inserted and linked to correct study
        assert len(Run.objects.all()) == 14
        assert len(AnnotationJob.objects.all()) == 14
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_raise_exception_if_mgys_accession_is_invalid(self):
        with pytest.raises(ValueError):
            creq.main(['MGYS_INVALID', 'Webin-460', '1', '--annotate', '--db', 'default', '--lineage',
                       'root:Host-Associated:Human'])

    def test_main_should_require_lineage_to_insert_run(self):
        with pytest.raises(ValueError):
            creq.main(['SRP077065', 'Webin-460', '1', '--annotate', '--db', 'default'])

    def test_main_should_create_assembly_job(self):
        Assembler(name='metaspades', version='3.12.0').save()
        AssemblyJobStatus(description='pending').save()
        creq.main(['SRP077065', 'Webin-460', '0', '--assemble', '--lineage', 'root', '--db', 'default'])
        assert len(Run.objects.all()) == 2
        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 2
        for job in assembly_jobs:
            assert job.assembler.name == 'metaspades'
            assert job.assembler.version == '3.12.0'
        assert len(UserRequest.objects.all()) == 1

    def test_main_should_throw_error_if_both_annotate_and_assemble_flags_not_given(self):
        with pytest.raises(SystemExit):
            creq.main(['SRP077065', 'Webin-460', '0', '--lineage', 'root', '--db', 'default'])

    def test_main_should_function_with_study_containing_only_assemblies(self):
        creq.main(['MGYS00003602', 'Webin-460', '0', '--lineage', 'root', '--db', 'default', '--annotate'])
        assert len(Study.objects.all()) == 1
        assert len(Assembly.objects.all()) == 1
        assert len(AnnotationJob.objects.all()) == 1

    def test_main_should_raise_error_with_study_containing_only_assemblies_if_no_lineage_provided(self):
        with pytest.raises(ValueError):
            creq.main(['MGYS00003602', 'Webin-460', '0', '--db', 'default', '--annotate'])


class TestCompleteRequestCLI(object):
    study_accession = 'ERP023889'
    webin_account = 'Webin-460'
    rt_ticket = 0

    @classmethod
    def setup_class(cls):
        clean_db()
        Pipeline(version=4.1).save()
        creq.main([cls.study_accession, cls.webin_account, cls.rt_ticket,
                   '--annotate', '--lineage', 'root', '--db', 'default'])
        assert len(AnnotationJob.objects.all()) == 2

    def setup_method(self, method):
        scheduled_status = AnnotationJobStatus.objects.get(description='SCHEDULED')
        AnnotationJob.objects.all().update(status=scheduled_status)

    def taredown_class(self, method):
        clean_db()

    def test_main_should_set_all_annotationjobs_to_completed(self):
        ccomp.main([self.study_accession, self.rt_ticket, '--db', 'default'])
        annotation_jobs = AnnotationJob.objects.all()
        for job in annotation_jobs:
            assert job.status.description == 'COMPLETED'

    def test_main_should_work_with_mgys_accession(self):
        mgys_accession = 'MGYS00001879'
        ccomp.main([mgys_accession, self.rt_ticket, '--db', 'default'])
        annotation_jobs = AnnotationJob.objects.all()
        for job in annotation_jobs:
            assert job.status.description == 'COMPLETED'

    def test_main_should_work_with_ena_primary_accession(self):
        mgys_accession = 'PRJEB21618'
        ccomp.main([mgys_accession, self.rt_ticket, '--db', 'default'])
        annotation_jobs = AnnotationJob.objects.all()
        for job in annotation_jobs:
            assert job.status.description == 'COMPLETED'

    def test_main_should_raise_error_if_secondary_accession_not_in_backlog(self):
        with pytest.raises(ValueError):
            ccomp.main(['ERP_INVALID', self.rt_ticket, '--db', 'default'])

    def test_main_should_raise_error_if_mgys_accession_not_in_backlog(self):
        with pytest.raises(ValueError):
            ccomp.main(['MGYS_invalid', self.rt_ticket, '--db', 'default'])

    def test_main_should_set_single_run_as_failed(self):
        mgys_accession = 'PRJEB21618'
        failed_accession = 'ERR2026004'
        ccomp.main([mgys_accession, self.rt_ticket, '--db', 'default', '--failed_runs', 'ERR2026004'])
        annotation_jobs = AnnotationJob.objects.all()
        for job in annotation_jobs:
            if job.runannotationjob_set.all()[0].run.primary_accession == failed_accession:
                expected_status = 'FAILED'
            else:
                expected_status = 'COMPLETED'
            assert job.status.description == expected_status

    def test_main_should_set_both_runs_as_failed(self):
        mgys_accession = 'PRJEB21618'
        ccomp.main([mgys_accession, self.rt_ticket, '--db', 'default', '--failed_runs', 'ERR2026003', 'ERR2026004'])
        annotation_jobs = AnnotationJob.objects.all()
        for job in annotation_jobs:
            assert job.status.description == 'FAILED'
