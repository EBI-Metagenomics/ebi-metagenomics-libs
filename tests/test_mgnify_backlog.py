from datetime import datetime
import pytest

from mgnify_backlog import mgnify_handler

from backlog.models import Study, Run, RunAssembly, AssemblyJob, Assembler, AssemblyJobStatus, RunAssemblyJob, \
    User, Pipeline, UserRequest, Assembly, AnnotationJob, AnnotationJobStatus
from ena_portal_api import ena_handler

from tests.util import user_data, clean_db, assembly_data, study_data, run_data
import copy


class MockResponse:
    def __init__(self, status_code, data=None, text=None):
        self.data = data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.data


mgnify = mgnify_handler.MgnifyHandler('default')

ena = ena_handler.EnaApiHandler()


def create_annotation_jobs_using_ena_services(rt_ticket=0, priority=0, version=4.1):
    study = mgnify.create_study_obj(study_data)
    accessions = ['ERR164407', 'ERR164408', 'ERR164409']
    lineage = 'root:Host-Associated:Human:Digestive System'

    runs = [mgnify.get_or_save_run(ena, accession, study=study, lineage=lineage) for accession in
            accessions]
    versions = [1.0, 2.0, 3.0, 4.0, 4.1, 5.0]
    for version in versions:
        Pipeline(version=version).save()

    user = mgnify.create_user(user_data['webin_id'], user_data['email_address'],
                              user_data['first_name'],
                              user_data['surname'])
    request = mgnify.create_user_request(user, priority, rt_ticket)

    assert len(AnnotationJob.objects.all()) == 0

    mgnify.create_annotation_job(request, runs[0], priority, version)
    mgnify.create_annotation_job(request, runs[1], priority)  # latest pipeline version
    mgnify.create_annotation_job(request, runs[2], priority)  # latest pipeline version
    return study, runs


def create_annotation_jobs_without_ena_services(rt_ticket=0, priority=0):
    study = mgnify.create_study_obj(study_data)
    run_accessions = ['ERR164407', 'ERR164408', 'ERR164409']

    for run_acc in run_accessions:
        run_data["run_accession"] = run_acc
        mgnify.create_run_obj(study, run_data)

    pipeline = Pipeline(version=4.1)
    pipeline.save()

    user = mgnify.create_user(user_data['webin_id'], user_data['email_address'],
                              user_data['first_name'],
                              user_data['surname'])
    request = mgnify.create_user_request(user, priority, rt_ticket)

    runs = []
    for run_acc in run_accessions:
        run = mgnify.get_backlog_run(run_acc)
        runs.append(run)
        mgnify.create_annotation_job(request, run, 4)
    return study, runs


class TestBacklogHandler(object):
    def setup_method(self, method):
        clean_db()

    def taredown_method(self, method):
        clean_db()

    def test_save_and_retrieve_study(self):
        mgnify.create_study_obj(study_data)
        study = mgnify.get_backlog_study(primary_accession=study_data['study_accession'])

        assert isinstance(study, Study)
        assert study.primary_accession == study_data['study_accession']
        assert study.secondary_accession == study_data['secondary_study_accession']
        assert study.title == study_data['study_title']
        assert study.public
        assert study.ena_last_update == datetime.strptime(study_data['last_updated'], "%Y-%m-%d").date()

    def test_get_study_by_secondary_accession(self):
        mgnify.create_study_obj(study_data)
        study = mgnify.get_backlog_study(secondary_accession=study_data['secondary_study_accession'])

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
        retrieved_study = mgnify.get_or_save_study(None, secondary_accession=study_data['secondary_study_accession'])

        assert isinstance(retrieved_study, Study)
        assert retrieved_study.pk == inserted_study.pk

    def test_get_or_save_study_should_fetch_from_ena(self):
        retrieved_study = mgnify.get_or_save_study(ena, secondary_accession=study_data['secondary_study_accession'])

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

    def test_get_or_save_assembly_should_find_existing_assembly(self):
        study = mgnify.create_study_obj(study_data)
        created_assembly = mgnify.create_assembly_obj(ena, study, assembly_data, public=True)
        retrieve_assembly = mgnify.get_or_save_assembly(ena, assembly_data['analysis_accession'], study)

        assert isinstance(retrieve_assembly, Assembly)
        assert retrieve_assembly.pk == created_assembly.pk

    def test_get_or_save_assembly_should_create_assembly(self):
        study = mgnify.create_study_obj(study_data)
        retrieved_assembly = mgnify.get_or_save_assembly(ena, assembly_data['analysis_accession'], assembly_data, study)

        assert isinstance(retrieved_assembly, Assembly)
        assert retrieved_assembly.ena_last_update == assembly_data['last_updated']

    def test_create_assembly_obj_no_related_runs(self):
        study = mgnify.create_study_obj(study_data)
        mgnify.create_assembly_obj(ena, study, assembly_data, public=True)
        assemblies = Assembly.objects.all()
        assert len(assemblies) == 1
        assembly = assemblies[0]
        assert assembly.primary_accession == assembly_data['analysis_accession']
        assert assembly.ena_last_update == datetime.strptime(assembly_data['last_updated'], "%Y-%m-%d").date()

    def test_create_assembly_obj_w_related_runs(self):
        study = mgnify.create_study_obj(study_data)
        run = mgnify.create_run_obj(study, run_data)
        assembly_data_w_run = {k: v for k, v in assembly_data.items()}
        assembly = mgnify.create_assembly_obj(ena, study, assembly_data_w_run, public=True)

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
        retrieved_assembly_job = mgnify.is_assembly_job_in_backlog(run_data['run_accession'], 'metaspades',
                                                                   '3.11.1')
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
        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'],
                                  user_data['first_name'],
                                  user_data['surname'])
        mgnify.create_user_request(user, 0, 1)
        requests = UserRequest.objects.all()
        assert len(requests) == 1
        request = requests[0]
        assert request.user.pk == user.pk
        assert request.priority == 0
        assert request.rt_ticket == 1

    def test_get_user_request_should_return_inserted_request(self):
        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'],
                                  user_data['first_name'],
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

    def test_get_pipeline_by_version_should_return_correct_pipeline(self):
        versions = [1.0, 2.0, 3.0, 4.0, 4.1, 5.0]
        for version in versions:
            Pipeline(version=version).save()
        assert mgnify.get_pipeline_by_version(5.0).version == 5.0

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
        assembly = mgnify.create_assembly_obj(ena, study, assembly_data, public=True)
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
        runs = create_annotation_jobs_using_ena_services()[1]

        up_to_date_runs = mgnify.get_up_to_date_run_annotation_jobs(study_data['secondary_study_accession'])
        assert len(runs) == len(up_to_date_runs)

        up_to_date_runs_v4 = mgnify.get_up_to_date_run_annotation_jobs(study_data['secondary_study_accession'], 4.1)
        assert len(runs) == len(up_to_date_runs_v4)

        up_to_date_runs_v5 = mgnify.get_up_to_date_run_annotation_jobs(study_data['secondary_study_accession'], 5.0)
        assert 0 == len(up_to_date_runs_v5)

    def test_get_up_to_date_assembly_annotation_jobs_should_retrieve_all_jobs_in_priority_order(self):
        study = mgnify.create_study_obj(study_data)
        accessions = ['ERZ795049', 'ERZ795050']
        assemblies = []
        for accession in accessions:
            data = copy.deepcopy(assembly_data)
            data['analysis_accession'] = accession
            assemblies.append(mgnify.get_or_save_assembly(ena, accession, data, study))
        pipeline = Pipeline(version=4.1)
        pipeline.save()

        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'],
                                  user_data['first_name'],
                                  user_data['surname'])
        request = mgnify.create_user_request(user, 0, 1)

        assert len(AnnotationJob.objects.all()) == 0

        mgnify.create_annotation_job(request, assemblies[0], 0)
        mgnify.create_annotation_job(request, assemblies[1], 1)

        up_to_date_assemblies_v4 = mgnify.get_up_to_date_assembly_annotation_jobs(
            study_data['secondary_study_accession'])
        assert len(up_to_date_assemblies_v4) == len(assemblies)

        up_to_date_assemblies_v4 = mgnify.get_up_to_date_assembly_annotation_jobs(
            study_data['secondary_study_accession'], 4.1)
        assert len(up_to_date_assemblies_v4) == len(assemblies)

        up_to_date_assemblies_v5 = mgnify.get_up_to_date_assembly_annotation_jobs(
            study_data['secondary_study_accession'], 5)
        assert 0 == len(up_to_date_assemblies_v5)

    def test_create_annotation_job_with_pipeline_version(self):
        """
            Test should prove that annotation jobs with the correct pipeline version have
            been created.
        :return:
        """
        rt_ticket = 1
        study, _ = create_annotation_jobs_using_ena_services(rt_ticket=rt_ticket, version=3.0)
        latest_version = mgnify.get_latest_pipeline().version

        for annotation_job in AnnotationJob.objects.all():
            assert annotation_job.pipeline.version in [3.0, latest_version]
            assert annotation_job.pipeline.version not in [1.0, 2.0, 4.0, 4.1]

    def test_set_annotation_jobs_completed_should_set_all_annotation_jobs_to_completed(self):
        rt_ticket = 1
        study, _ = create_annotation_jobs_using_ena_services(rt_ticket=rt_ticket)

        mgnify.set_annotation_jobs_completed(study, rt_ticket)
        for annotation_job in AnnotationJob.objects.all():
            assert annotation_job.status.description == 'COMPLETED'

    def test_set_annotation_jobs_completed_should_not_set_excluded_accessions(self):
        rt_ticket = 1
        study, runs = create_annotation_jobs_using_ena_services(rt_ticket=rt_ticket)
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
        study, runs = create_annotation_jobs_using_ena_services(rt_ticket=rt_ticket)
        failed_accession = [runs[0].primary_accession]

        mgnify.set_annotation_jobs_failed(study, rt_ticket, failed_accession)

        for annotation_job in AnnotationJob.objects.all():
            if annotation_job.runannotationjob_set.first().run.primary_accession in failed_accession:
                status = 'FAILED'
            else:
                status = 'SCHEDULED'
            assert annotation_job.status.description == status

    def test_get_request_webin_should_retrieve_webin_account_associated_to_rt_ticket_number(self):
        user = mgnify.create_user(user_data['webin_id'], user_data['email_address'],
                                  user_data['first_name'],
                                  user_data['surname'])
        rt_ticket = 0
        _ = mgnify.create_user_request(user, 0, rt_ticket)
        assert mgnify.get_request_webin(rt_ticket) == user_data['webin_id']

    def test_update_annotation_jobs_status_should_raise_exception_on_invalid_status_description(self):
        with pytest.raises(ValueError):
            mgnify.update_annotation_jobs_status([], 'INVALID_DESCRIPTION')

    def test_update_annotation_jobs_should_set_priority_and_status_for_all_runs_in_study(self):
        rt_ticket = 0
        initial_priority = 1
        initial_status = AnnotationJobStatus.objects.get(description='SCHEDULED')

        final_priority = 2
        final_status_description = 'RUNNING'

        assert len(AnnotationJob.objects.all()) == 0

        study, _ = create_annotation_jobs_using_ena_services(rt_ticket, initial_priority)
        study_secondary_accession = study.secondary_accession

        initial_jobs = AnnotationJob.objects.all()
        for job in initial_jobs:
            assert job.priority == initial_priority
            assert job.status == initial_status

        mgnify.update_annotation_jobs_from_accessions(study_accessions=[study_secondary_accession],
                                                      priority=final_priority,
                                                      status_description=final_status_description)

        final_jobs = AnnotationJob.objects.all()
        assert len(initial_jobs) == len(final_jobs)
        for job in final_jobs:
            assert job.priority == final_priority
            assert job.status.description == final_status_description

    def test_update_annotation_jobs_should_set_priority_and_status_for_all_runs_in_study_filtered(self):
        rt_ticket = 0
        initial_priority = 1
        initial_status = AnnotationJobStatus.objects.get(description='SCHEDULED')

        filtered_run_accession = ['ERR164407']

        final_priority = 2
        final_status_description = 'RUNNING'

        assert len(AnnotationJob.objects.all()) == 0

        study, _ = create_annotation_jobs_using_ena_services(rt_ticket, initial_priority)
        study_secondary_accession = study.secondary_accession

        initial_jobs = AnnotationJob.objects.all()
        for job in initial_jobs:
            assert job.priority == initial_priority
            assert job.status == initial_status

        mgnify.update_annotation_jobs_from_accessions(study_accessions=[study_secondary_accession],
                                                      run_or_assembly_accessions=filtered_run_accession,
                                                      priority=final_priority,
                                                      status_description=final_status_description)

        final_jobs = AnnotationJob.objects.all()
        assert len(initial_jobs) == len(final_jobs)
        for job in final_jobs:
            if job.runannotationjob_set.first().run.primary_accession in filtered_run_accession:
                assert job.priority == final_priority
                assert job.status.description == final_status_description
            else:
                assert job.priority == initial_priority
                assert job.status == initial_status

    def test_update_annotation_jobs_should_set_priority_and_status_for_run_only(self):
        rt_ticket = 0
        initial_priority = 1
        initial_status = AnnotationJobStatus.objects.get(description='SCHEDULED')

        final_priority = 2
        final_status_description = 'RUNNING'

        assert len(AnnotationJob.objects.all()) == 0

        create_annotation_jobs_using_ena_services(rt_ticket, initial_priority)

        filtered_run_accession = ['ERR164407']

        initial_jobs = AnnotationJob.objects.all()
        for job in initial_jobs:
            assert job.priority == initial_priority
            assert job.status == initial_status

        mgnify.update_annotation_jobs_from_accessions(run_or_assembly_accessions=filtered_run_accession,
                                                      priority=final_priority,
                                                      status_description=final_status_description)

        final_jobs = AnnotationJob.objects.all()
        assert len(initial_jobs) == len(final_jobs)
        for job in final_jobs:
            if job.runannotationjob_set.first().run.primary_accession in filtered_run_accession:
                assert job.priority == final_priority
                assert job.status.description == final_status_description
            else:
                assert job.priority == initial_priority
                assert job.status == initial_status

    def test_update_annotation_jobs_should_update_by_pipeline_version_only(self):
        rt_ticket = 0
        initial_priority = 1
        initial_status = AnnotationJobStatus.objects.get(description='SCHEDULED')

        final_priority = 2
        final_status_description = 'RUNNING'

        new_pipeline_version = 5.0

        assert len(AnnotationJob.objects.all()) == 0

        create_annotation_jobs_using_ena_services(rt_ticket, initial_priority)
        assert len(AnnotationJob.objects.all()) == 3

        Pipeline(version=new_pipeline_version).save()

        request = UserRequest.objects.first()
        for run in Run.objects.all():
            mgnify.create_annotation_job(request=request, assembly_or_run=run, priority=initial_priority)

        assert len(AnnotationJob.objects.all()) == 6

        initial_jobs = AnnotationJob.objects.all()
        for job in initial_jobs:
            assert job.priority == initial_priority
            assert job.status == initial_status

        mgnify.update_annotation_jobs_from_accessions(priority=final_priority,
                                                      status_description=final_status_description,
                                                      pipeline_version=new_pipeline_version)

        final_jobs = AnnotationJob.objects.all()
        assert len(initial_jobs) == len(final_jobs)
        for job in final_jobs:
            if job.pipeline.version == new_pipeline_version:
                assert job.priority == final_priority
                assert job.status.description == final_status_description
            else:
                assert job.priority == initial_priority
                assert job.status == initial_status

    def test_update_annotationjob(self):
        rt_ticket = 0
        initial_priority = 1
        assert len(AnnotationJob.objects.all()) == 0
        create_annotation_jobs_using_ena_services(rt_ticket, initial_priority)

        running_status = mgnify.get_annotation_job_status('RUNNING')
        job = AnnotationJob.objects.first()
        job_id = job.id

        new_attr = {'priority': 4, 'status': running_status}
        mgnify.update_annotation_job(job, new_attr)

        modified_job = AnnotationJob.objects.get(id=job_id)

        for attr, val in new_attr.items():
            assert getattr(modified_job, attr) == val

    def test_get_annotation_jobs_status_descriptions_str(self):
        assert 0 == len(AnnotationJob.objects.all()) == 0

        create_annotation_jobs_without_ena_services()

        assert 3 == len(AnnotationJob.objects.all())
        jobs = mgnify.get_annotation_jobs(status_descriptions='SCHEDULED')
        assert 3 == len(jobs)

    def test_get_annotation_jobs_status_descriptions_list(self):
        assert 0 == len(AnnotationJob.objects.all()) == 0

        create_annotation_jobs_without_ena_services()

        assert 3 == len(AnnotationJob.objects.all())
        jobs = mgnify.get_annotation_jobs(status_descriptions=['SCHEDULED', 'RUNNING'])
        assert 3 == len(jobs)

    def test_get_annotation_jobs_status_descriptions_should_return_no_jobs(self):
        assert 0 == len(AnnotationJob.objects.all()) == 0

        create_annotation_jobs_without_ena_services()

        assert 3 == len(AnnotationJob.objects.all())
        jobs = mgnify.get_annotation_jobs(status_descriptions=['RUNNING'])
        assert 0 == len(jobs)
