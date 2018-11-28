import pytest
from datetime import datetime

from mgnify_backlog import mgnify_handler
from ena_portal_api import ena_handler

from backlog.models import Study, Run, RunAssembly, AssemblyJob, Assembler, AssemblyJobStatus, RunAssemblyJob, \
    User, Pipeline, UserRequest, Assembly, AnnotationJob

from tests.util import clean_db, study_data, run_data, assembly_data, user_data


class MockResponse:
    def __init__(self, status_code, data=None, text=None):
        self.data = data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.data


mgnify = mgnify_handler.MgnifyHandler('default')

ena = ena_handler.EnaApiHandler()


def create_annotation_jobs(rt_ticket=0, priority=0):
    study = mgnify.create_study_obj(study_data)
    accessions = ['ERR164407', 'ERR164408', 'ERR164409']
    lineage = 'root:Host-Associated:Human:Digestive System'

    runs = [mgnify.get_or_save_run(ena, accession, study=study, lineage=lineage) for accession in accessions]
    pipeline = Pipeline(version=4.1)
    pipeline.save()

    user = mgnify.create_user(user_data['webin_id'], user_data['email_address'], user_data['first_name'],
                              user_data['surname'])
    request = mgnify.create_user_request(user, priority, rt_ticket)

    assert len(AnnotationJob.objects.all()) == 0

    mgnify.create_annotation_job(request, runs[0], 0)
    mgnify.create_annotation_job(request, runs[1], 1)
    mgnify.create_annotation_job(request, runs[2], 2)
    return study, runs


class TestBacklogHandler(object):
    def setup_method(self, method):
        clean_db()

    def taredown_method(self, method):
        clean_db()

    def test_save_and_retrieve_study(self):
        mgnify.create_study_obj(study_data)
        study = mgnify.get_backlog_study(study_data['study_accession'])

        assert isinstance(study, Study)
        assert study.primary_accession == study_data['study_accession']
        assert study.secondary_accession == study_data['secondary_study_accession']
        assert study.title == study_data['study_title']
        assert study.public
        assert study.ena_last_update == datetime.strptime(study_data['last_updated'], "%Y-%m-%d").date()

    def test_get_study_by_secondary_accession(self):
        mgnify.create_study_obj(study_data)
        study = mgnify.get_backlog_study(study_data['secondary_study_accession'])

        assert isinstance(study, Study)
        assert study.primary_accession == study_data['study_accession']
        assert study.secondary_accession == study_data['secondary_study_accession']
        assert study.title == study_data['study_title']
        assert study.public
        assert study.ena_last_update == datetime.strptime(study_data['last_updated'], "%Y-%m-%d").date()

    def test_save_and_retrieve_run(self):
        study = mgnify.create_study_obj(study_data)
        mgnify.create_run_obj(study, run_data)
        run = mgnify.get_backlog_run(run_data['run_accession'])

        assert isinstance(run, Run)
        assert run.primary_accession == run_data['run_accession']
        assert run.base_count == run_data['base_count']
        assert run.read_count == run_data['read_count']
        assert run.instrument_platform == run_data['instrument_platform']
        assert run.instrument_model == run_data['instrument_model']
        assert run.library_strategy == run_data['library_strategy']
        assert run.library_layout == run_data['library_layout']
        assert run.library_source == run_data['library_source']
        assert run.ena_last_update == datetime.strptime(run_data['last_updated'], "%Y-%m-%d").date()

    def test_get_or_save_study_should_find_existing_study(self):
        inserted_study = mgnify.create_study_obj(study_data)
        retrieved_study = mgnify.get_or_save_study(None, study_data['secondary_study_accession'])

        assert isinstance(retrieved_study, Study)
        assert retrieved_study.pk == inserted_study.pk

    def test_get_or_save_study_should_fetch_from_ena(self):
        retrieved_study = mgnify.get_or_save_study(ena, study_data['secondary_study_accession'])

        assert isinstance(retrieved_study, Study)
        assert len(Study.objects.all()) == 1
        assert retrieved_study.primary_accession == study_data['study_accession']
        assert retrieved_study.title == study_data['study_title']

    def test_get_or_save_run_should_find_existing_run(self):
        study = mgnify.create_study_obj(study_data)
        created_run = mgnify.create_run_obj(study, run_data)
        retrieved_run = mgnify.get_or_save_run(ena, run_data['run_accession'], study=study)

        assert isinstance(retrieved_run, Run)
        assert retrieved_run.pk == created_run.pk

    def test_get_or_save_run_should_fetch_from_ena(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.get_or_save_run(ena, run_data['run_accession'], study=study)

        assert isinstance(run, Run)
        assert run.primary_accession == run_data['run_accession']
        assert run.base_count == run_data['base_count']
        assert run.read_count == run_data['read_count']
        assert run.instrument_platform == run_data['instrument_platform']
        assert run.instrument_model == run_data['instrument_model']
        assert run.library_strategy == run_data['library_strategy']
        assert run.library_layout == run_data['library_layout']
        assert run.library_source == run_data['library_source']
        # ena_last_update; last date on which row was updated from ENA
        assert run.ena_last_update == datetime.today().date()

    # def test_get_or_save_run_should_require_lineage_to_insert_run(self):
    #     study = mgnify.create_study_obj(study_data)
    #     with pytest.raises(ValueError):
    #         mgnify.get_or_save_run(ena, run_data['run_accession'], None, study)

    def test_get_or_save_assembly_should_find_existing_assembly(self):
        study = mgnify.create_study_obj(study_data)
        created_assembly = mgnify.create_assembly_obj(study, assembly_data)
        retrieve_assembly = mgnify.get_or_save_assembly(ena, assembly_data['accession'], study)

        assert isinstance(retrieve_assembly, Assembly)
        assert retrieve_assembly.pk == created_assembly.pk

    def test_get_or_save_assembly_should_fetch_from_ena(self):
        study = mgnify.create_study_obj(study_data)
        retrieved_assembly = mgnify.get_or_save_assembly(ena, assembly_data['accession'], study)

        assert isinstance(retrieved_assembly, Assembly)
        assert retrieved_assembly.ena_last_update == assembly_data['last_updated']

    # def test_get_or_save_assembly_should_require_lineage_to_insert_assembly(self):
    #     study = mgnify.create_study_obj(study_data)
    #     with pytest.raises(ValueError):
    #         mgnify.get_or_save_assembly(ena, assembly_data['accession'], None, study)

    def test_create_assembly_obj_no_related_runs(self):
        study = mgnify.create_study_obj(study_data)
        mgnify.create_assembly_obj(study, assembly_data)
        assemblies = Assembly.objects.all()
        assert len(assemblies) == 1
        assembly = assemblies[0]
        assert assembly.primary_accession == assembly_data['accession']
        assert assembly.ena_last_update == datetime.strptime(assembly_data['last_updated'], "%Y-%m-%d").date()

    def test_create_assembly_obj_w_related_runs(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)
        assembly_data_w_run = {k: v for k, v in assembly_data.items()}
        assembly_data_w_run['related_runs'] = [run]
        assembly = mgnify.create_assembly_obj(study, assembly_data_w_run)

        run_assemblies = RunAssembly.objects.all()
        assert len(run_assemblies) == 1
        run_assembly = run_assemblies[0]
        assert run_assembly.run.pk == run.pk
        assert run_assembly.assembly.pk == assembly.pk

    def test_create_assembly_job_should_set_latest_assembler(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)
        latest_version = '3.12.0'
        _ = Assembler(name='metaspades', version='3.11.1').save()
        _ = Assembler(name='metaspades', version=latest_version).save()

        status = AssemblyJobStatus(description='PENDING')
        status.save()
        mgnify.create_assembly_job(run, 0, status, 'metaspades')

        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 1
        assembly_job = assembly_jobs[0]
        assert assembly_job.assembler.version == latest_version

    def test_create_assembly_job_should_set_status_from_model(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)
        _ = Assembler(name='metaspades', version='3.11.1').save()

        status = AssemblyJobStatus(description='PENDING')
        status.save()
        mgnify.create_assembly_job(run, 0, status, 'metaspades')

        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 1
        assembly_job = assembly_jobs[0]
        assert assembly_job.status.description == status.description

    def test_create_assembly_job_should_set_status_from_string_arg(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)
        description = 'PENDING'

        _ = Assembler(name='metaspades', version='3.11.1').save()
        _ = AssemblyJobStatus(description=description).save()
        mgnify.create_assembly_job(run, 0, description, 'metaspades')

        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 1
        assembly_job = assembly_jobs[0]
        assert assembly_job.status.description == description

    def test_is_assembly_in_backlog_should_not_find_any_assemblies(self):
        assert mgnify.is_assembly_job_in_backlog('ERR12345_test', 'metaspades', '3.11.1') is None

    def test_is_assembly_in_backlog_should_not_find_assembly_version(self):
        study = mgnify.create_study_obj(study_data)

        status = AssemblyJobStatus(description='pending')
        status.save()

        run = mgnify.create_run_obj(study, run_data)

        mgnify.create_assembly_job(run, '0', status, 'metaspades', '3.12.0')
        assert mgnify.is_assembly_job_in_backlog(run_data['run_accession'], 'metaspades', '3.11.1') is None

    def test_is_assembly_in_backlog_should_find_assembly_version(self):
        study = mgnify.create_study_obj(study_data)

        status = AssemblyJobStatus(description='pending')
        status.save()

        run = mgnify.create_run_obj(study, run_data)

        inserted_assembly_job = mgnify.create_assembly_job(run, '0', status, 'metaspades', '3.11.1')
        retrieved_assembly_job = mgnify.is_assembly_job_in_backlog(run_data['run_accession'], 'metaspades', '3.11.1')
        assert inserted_assembly_job.pk == retrieved_assembly_job.pk

    def test_is_assembly_in_backlog_should_find_any_version(self):
        study = mgnify.create_study_obj(study_data)

        status = AssemblyJobStatus(description='pending')
        status.save()

        run = mgnify.create_run_obj(study, run_data)

        inserted_assembly_job = mgnify.create_assembly_job(run, '0', status, 'metaspades', '3.11.1')
        retrieved_assembly_job = mgnify.is_assembly_job_in_backlog(run_data['run_accession'], 'metaspades')
        assert inserted_assembly_job.pk == retrieved_assembly_job.pk

    def test_get_user_should_retrieve_user(self):
        User(**user_data).save()
        user = mgnify.get_user(user_data['webin_id'])
        for v, k in user_data.items():
            assert getattr(user, v) == k

    def test_create_user_should_create_user(self):
        mgnify.create_user(user_data['webin_id'], user_data['email_address'], user_data['first_name'],
                           user_data['surname'])
        users = User.objects.all()
        assert len(users) == 1
        user = users[0]
        for v, k in user_data.items():
            assert getattr(user, v) == k

    def test_create_user_request_should_create_user_request(self):
        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'], user_data['first_name'],
                                  user_data['surname'])
        mgnify.create_user_request(user, 0, 1)
        requests = UserRequest.objects.all()
        assert len(requests) == 1
        request = requests[0]
        assert request.user.pk == user.pk
        assert request.priority == 0
        assert request.rt_ticket == 1

    def test_get_user_request_should_return_inserted_request(self):
        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'], user_data['first_name'],
                                  user_data['surname'])
        inserted_request = UserRequest(user=user, rt_ticket=1234, priority=1)
        inserted_request.save()
        retrieved_request = mgnify.get_user_request(1234)
        assert retrieved_request.pk == inserted_request.pk

    def test_get_latest_pipeline_should_return_latest_pipeline(self):
        versions = [1.0, 2.0, 3.0, 4.0, 4.1]
        for version in versions:
            Pipeline(version=version).save()
        assert mgnify.get_latest_pipeline().version == max(versions)

    def test_create_annotation_job_should_create_annotationjob_for_run(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)
        Pipeline(version=4.1).save()

        user = User(**user_data)
        user.save()
        request = UserRequest(user=user, rt_ticket=1234, priority=1)
        request.save()

        inserted_annotation_job = mgnify.create_annotation_job(request, run, 1)
        retrieved_annotation_jobs = AnnotationJob.objects.all()
        assert len(retrieved_annotation_jobs) == 1
        retrieved_annotation_job = retrieved_annotation_jobs[0]

        assert inserted_annotation_job.pk == retrieved_annotation_job.pk
        related_runs = retrieved_annotation_job.runannotationjob_set.all()
        assert len(related_runs) == 1
        assert isinstance(related_runs[0].run, Run)
        assert related_runs[0].run.pk == run.pk

    def test_create_annotation_job_should_create_annotationjob_for_assembly(self):
        study = mgnify.create_study_obj(study_data)
        assembly = mgnify.create_assembly_obj(study, assembly_data)
        Pipeline(version=4.1).save()

        user = User(**user_data)
        user.save()
        request = UserRequest(user=user, rt_ticket=1234, priority=1)
        request.save()

        inserted_annotation_job = mgnify.create_annotation_job(request, assembly, 1)
        retrieved_annotation_jobs = AnnotationJob.objects.all()
        assert len(retrieved_annotation_jobs) == 1
        retrieved_annotation_job = retrieved_annotation_jobs[0]

        assert inserted_annotation_job.pk == retrieved_annotation_job.pk
        related_assemblies = retrieved_annotation_job.assemblyannotationjob_set.all()
        assert len(related_assemblies) == 1
        assert isinstance(related_assemblies[0].assembly, Assembly)
        assert related_assemblies[0].assembly.pk == assembly.pk

    def test_save_assembly_job_should_create_new_job(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)
        status = AssemblyJobStatus(description='pending')
        status.save()

        job = mgnify.save_assembly_job(run, 1, 'metaspades', '3.12.0', status, 3)
        jobs = AssemblyJob.objects.all()
        assert len(jobs) == 1
        inserted_job = jobs[0]
        assert inserted_job.pk == job.pk
        assert inserted_job.input_size == 1
        assert inserted_job.assembler.name == 'metaspades'
        assert inserted_job.assembler.version == '3.12.0'
        assert inserted_job.status.pk == status.pk
        assert inserted_job.priority == 3

    def test_save_assembly_job_should_update_job(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)

        assembler = Assembler(name='metaspades', version='3.12.0')
        assembler.save()
        status = AssemblyJobStatus(description='pending')
        status.save()
        status2 = AssemblyJobStatus(description='running')
        status2.save()

        assert len(AssemblyJob.objects.all()) == 0
        assembly_job = AssemblyJob(input_size=0, assembler=assembler, status=status, priority=0)
        assembly_job.save()
        RunAssemblyJob(run=run, assembly_job=assembly_job).save()

        new_priority = 1
        mgnify.save_assembly_job(run, 0, assembler.name, assembler.version, status2, new_priority)

        assert len(RunAssemblyJob.objects.all()) == 1
        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 1
        updated_assembly_job = assembly_jobs[0]
        assert updated_assembly_job.pk == assembly_job.pk
        assert updated_assembly_job.status.pk == status2.pk
        assert updated_assembly_job.priority == new_priority

    def test_set_assembly_job_running_should_update_existing_assembly_job(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)

        status = AssemblyJobStatus(description='pending')
        status.save()

        AssemblyJobStatus(description='running').save()

        assembler_name = 'metaspades'
        assembler_version = '3.11.1'
        inserted_assembly_job = mgnify.create_assembly_job(run, '0', status, assembler_name, assembler_version)

        assert len(AssemblyJob.objects.all()) == 1
        mgnify.set_assembly_job_running(run_data['run_accession'], assembler_name, assembler_version)

        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 1
        assert len(Run.objects.all()) == 1
        assert len(Study.objects.all()) == 1
        assembly_job = assembly_jobs[0]
        assert assembly_job.pk == inserted_assembly_job.pk
        assert assembly_job.status.description == 'running'

    def test_set_assemblyjob_pending_should_update_existing_assembly_job(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)

        status = AssemblyJobStatus(description='pending')
        status.save()

        run_status = AssemblyJobStatus(description='running')
        run_status.save()

        assembler_name = 'metaspades'
        assembler_version = '3.11.1'
        inserted_assembly_job = mgnify.create_assembly_job(run, '0', run_status, assembler_name, assembler_version)

        assert len(AssemblyJob.objects.all()) == 1
        mgnify.set_assembly_job_pending(run_data['run_accession'], assembler_name, assembler_version)

        assembly_jobs = AssemblyJob.objects.all()
        assert len(assembly_jobs) == 1
        assert len(Run.objects.all()) == 1
        assert len(Study.objects.all()) == 1
        assembly_job = assembly_jobs[0]
        assert assembly_job.pk == inserted_assembly_job.pk
        assert assembly_job.status.description == 'pending'

    def test_filter_active_runs_should_return_empty_list(self):
        assert len(AssemblyJob.objects.all()) == 0
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)

        status = AssemblyJobStatus(description='pending')
        status.save()

        mgnify.create_assembly_job(run, '0', status, 'metaspades', '3.12.0')
        assert len(AssemblyJob.objects.all()) == 1
        assert len(mgnify.filter_active_runs([run_data], 'metaspades')) == 0

    def test_filter_active_runs_should_return_run_as_no_assembly_jobs_in_db(self):
        assert len(mgnify.filter_active_runs([run_data], 'metaspades')) == 1

    def test_get_latest_assembler_version_should_return_latest_version(self):
        assembler_name = 'metaspades'
        versions = ['3.10.0', '3.11.1', '3.12.0']
        for version in versions:
            Assembler(name=assembler_name, version=version).save()
        assert mgnify.get_latest_assembler_version(assembler_name) == '3.12.0'

    def test_get_latest_assembler_version_should_raise_exception_if_not_found(self):
        with pytest.raises(mgnify_handler.ObjectDoesNotExist):
            mgnify.get_latest_assembler_version('metaspades')

    def test_get_pending_assembly_jobs_should_return_empty_list(self):
        assert len(mgnify.get_pending_assembly_jobs()) == 0

    def test_get_pending_assembly_jobs_should_return_all_pending_jobs(self):
        status = AssemblyJobStatus(description='pending')
        status.save()

        assembler = Assembler(name='metaspades', version='3.12.0')
        assembler.save()

        assert len(AssemblyJob.objects.all()) == 0
        AssemblyJob(directory='/dir', status=status, priority=0, assembler=assembler, input_size=3).save()
        AssemblyJob(directory='/dir', status=status, priority=1, assembler=assembler, input_size=4).save()
        assert len(mgnify.get_pending_assembly_jobs()) == 2

    def test_get_pending_assembly_jobs_should_order_jobs_by_decreasing_priority(self):
        status = AssemblyJobStatus(description='pending')
        status.save()

        assembler = Assembler(name='metaspades', version='3.12.0')
        assembler.save()

        assert len(AssemblyJob.objects.all()) == 0
        job1 = AssemblyJob(directory='/dir', status=status, priority=3, assembler=assembler, input_size=3)
        job1.save()
        job2 = AssemblyJob(directory='/dir', status=status, priority=1, assembler=assembler, input_size=4)
        job2.save()

        pending_jobs = mgnify.get_pending_assembly_jobs()
        assert len(pending_jobs) == 2
        assert pending_jobs[0].pk == job1.pk
        assert pending_jobs[1].pk == job2.pk

    def test_is_valid_lineage_should_return_true_as_lineage_exists(self):
        assert mgnify.is_valid_lineage('root:Environmental')

    def test_is_valid_lineage_should_return_false_as_lineage_exists(self):
        assert not mgnify.is_valid_lineage('root:Environmen')

    def test_get_up_to_date_run_annotation_jobs_should_retrieve_all_jobs_in_priority_order(self):
        runs = create_annotation_jobs()[1]

        up_to_date_runs = mgnify.get_up_to_date_run_annotation_jobs(study_data['secondary_study_accession'])
        assert len(up_to_date_runs) == len(runs)

    def test_get_up_to_date_assembly_annotation_jobs_should_retrieve_all_jobs_in_priority_order(self):
        study = mgnify.create_study_obj(study_data)
        accessions = ['GCA_001751075', 'GCA_001751165']
        lineage = 'root:Host-Associated:Human:Digestive System'

        assemblies = [mgnify.get_or_save_assembly(ena, accession, study) for accession in accessions]
        pipeline = Pipeline(version=4.1)
        pipeline.save()

        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'], user_data['first_name'],
                                  user_data['surname'])
        request = mgnify.create_user_request(user, 0, 1)

        assert len(AnnotationJob.objects.all()) == 0

        mgnify.create_annotation_job(request, assemblies[0], 0)
        mgnify.create_annotation_job(request, assemblies[1], 1)

        up_to_date_assemblies = mgnify.get_up_to_date_assembly_annotation_jobs(study_data['secondary_study_accession'])
        assert len(up_to_date_assemblies) == len(assemblies)

    def test_set_annotation_jobs_completed_should_set_all_annotation_jobs_to_completed(self):
        rt_ticket = 1
        study, _ = create_annotation_jobs(rt_ticket=rt_ticket)

        mgnify.set_annotation_jobs_completed(study, rt_ticket)
        for annotation_job in AnnotationJob.objects.all():
            assert annotation_job.status.description == 'COMPLETED'

    def test_set_annotation_jobs_completed_should_not_set_excluded_accessions(self):
        rt_ticket = 1
        study, runs = create_annotation_jobs(rt_ticket=rt_ticket)
        excluded_accession = [runs[0].primary_accession]

        mgnify.set_annotation_jobs_completed(study, rt_ticket, excluded_runs=excluded_accession)

        for annotation_job in AnnotationJob.objects.all():

            if annotation_job.runannotationjob_set.first().run.primary_accession in excluded_accession:
                status = 'SCHEDULED'
            else:
                status = 'COMPLETED'
            assert annotation_job.status.description == status

    def test_set_annotation_jobs_failed_should_filter_by_accession(self):
        rt_ticket = 1
        study, runs = create_annotation_jobs(rt_ticket=rt_ticket)
        failed_accession = [runs[0].primary_accession]

        mgnify.set_annotation_jobs_failed(study, rt_ticket, failed_accession)

        for annotation_job in AnnotationJob.objects.all():
            if annotation_job.runannotationjob_set.first().run.primary_accession in failed_accession:
                status = 'FAILED'
            else:
                status = 'SCHEDULED'
            assert annotation_job.status.description == status

    def test_get_request_webin_should_retrieve_webin_account_associated_to_rt_ticket_number(self):
        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'], user_data['first_name'],
                                  user_data['surname'])
        rt_ticket = 0
        _ = mgnify.create_user_request(user, 0, rt_ticket)
        assert mgnify.get_request_webin(rt_ticket) == user_data['webin_id']
