import pytest

from analysis_request_cli import edit_annotation_job

from backlog.models import Study, Run, AssemblyJob, Assembler, AssemblyJobStatus, AnnotationJobStatus, \
    Pipeline, UserRequest, AnnotationJob, Assembly

import mgnify_backlog.mgnify_handler as mgnify_handler
from tests.util import clean_db, create_annotation_jobs, study_data, run_data

mgnify = mgnify_handler.MgnifyHandler('default')


class TestCreateRequestCLI(object):
    def setup_method(self, method):
        clean_db()
        Pipeline(version=4.1).save()

    def taredown_method(self, method):
        clean_db()

    def test_edit_annotation_jobs_should_set_status(self):
        rt_ticket = 0
        initial_priority = 1
        initial_status = AnnotationJobStatus.objects.get(description='SCHEDULED')

        final_priority = 2
        final_status_description = 'RUNNING'

        assert len(AnnotationJob.objects.all()) == 0

        study, _ = create_annotation_jobs(rt_ticket, initial_priority)

        initial_jobs = AnnotationJob.objects.all()
        for job in initial_jobs:
            assert job.priority == initial_priority
            assert job.status == initial_status

        edit_annotation_job.main(
            ['-s', study_data['secondary_study_accession'], '-ss', final_status_description, '-p', str(final_priority)])

        final_jobs = AnnotationJob.objects.all()
        assert len(initial_jobs) == len(final_jobs)
        for job in final_jobs:
            assert job.priority == final_priority
            assert job.status.description == final_status_description

    def test_edit_annotation_jobs_should_set_status_of_runs(self):
        rt_ticket = 0
        initial_priority = 1
        initial_status = AnnotationJobStatus.objects.get(description='SCHEDULED')

        final_priority = 2
        final_status_description = 'RUNNING'

        run_accession = 'ERR164407,ERR164408'
        assert len(AnnotationJob.objects.all()) == 0

        study, _ = create_annotation_jobs(rt_ticket, initial_priority)

        initial_jobs = AnnotationJob.objects.all()
        for job in initial_jobs:
            assert job.priority == initial_priority
            assert job.status == initial_status

        edit_annotation_job.main(
            ['-ra', run_accession, '-ss', final_status_description, '-p', str(final_priority)])

        final_jobs = AnnotationJob.objects.all()
        assert len(initial_jobs) == len(final_jobs)
        for job in final_jobs:
            if job.runannotationjob_set.first().run.primary_accession in run_accession:
                assert job.priority == final_priority
                assert job.status.description == final_status_description
            else:
                assert job.priority == initial_priority
                assert job.status == initial_status

    def test_edit_annnotation_jobs_should_exit_if_no_runs_or_studies_specified(self):
        with pytest.raises(SystemExit):
            edit_annotation_job.main(['-ss', 'DESCRIBED'])

    def test_edit_annotation_jobs_should_exit_if_no_status_or_priority_specified(self):
        with pytest.raises(SystemExit):
            edit_annotation_job.main(['-s', study_data['secondary_study_accession']])
