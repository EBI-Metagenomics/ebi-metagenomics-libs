from datetime import datetime
import os
import logging


import django.db
from django.core.exceptions import ObjectDoesNotExist

os.environ['DJANGO_SETTINGS_MODULE'] = 'db.settings'

django.setup()

from backlog.models import Study, Run, AssemblyJob, Assembler, AssemblyJobStatus, RunAssemblyJob

class MgnifyHandler:
    def __init__(self, database):
        self.database = database

    def create_study_obj(self, data):
        s = Study(primary_accession=data['study_accession'],
                  secondary_accession=data['secondary_study_accession'],
                  title=sanitise_string(data['study_title']),
                  public=get_date(data, 'first_public') <= datetime.now().date(),
                  ena_last_update=get_date(data, 'last_updated'),
                  )
        s.save(using=self.database)
        return s

    def create_run_obj(self, study, run):
        r = Run(study=study,
                primary_accession=run['run_accession'],
                base_count=run['base_count'],
                read_count=run['read_count'],
                instrument_platform=sanitise_string(run['instrument_platform']),
                instrument_model=sanitise_string(run['instrument_model']),
                library_strategy=sanitise_string(run['library_strategy']),
                library_layout=sanitise_string(run['library_layout']),
                library_source=sanitise_string(run['library_source']),
                ena_last_update=get_date(run, 'last_updated'),
                biome_validated=False
                )
        r.clean_fields()
        r.save(using=self.database)
        return r

    # def get_assemblies(database, secondary_study_accession, run_accessions, assembler, version):
    #     return Run.objects.using(database).filter()

    def is_study_in_backlog(self, secondary_study_accession):
        return Study.objects.using(self.database).filter(secondary_accession=secondary_study_accession)[0]

    def is_run_in_backlog(self, run_accession):
        return Run.objects.using(self.database).filter(primary_accession=run_accession)[0]

    def save_study(self, ena_handler, secondary_study_accession):
        try:
            return self.is_study_in_backlog(secondary_study_accession)
        except IndexError:
            study = ena_handler.get_study(secondary_study_accession)
            return self.create_study_obj(study)

    def save_run(self, study, run):
        try:
            return self.is_run_in_backlog(run['run_accession'])
        except IndexError:
            return self.create_run_obj(study, run)

    def is_assembly_job_in_backlog(self, primary_accession, assembler_name, assembler_version=None):
        if not assembler_version:
            jobs = AssemblyJob.objects.using(self.database).filter(runs__primary_accession=primary_accession,
                                                                   assembler__name=assembler_name)
        else:
            jobs = AssemblyJob.objects.using(self.database).filter(runs__primary_accession=primary_accession,
                                                                   assembler__name=assembler_name,
                                                                   assembler__version=assembler_version)
        return jobs[0] if len(jobs) > 0 else None

    def create_assembly_job(self, run, total_size, assembler_name, assembler_version, status, priority=0):
        try:
            assembler = Assembler.objects.using(self.database).get(name=assembler_name, version=assembler_version)
        except ObjectDoesNotExist:
            assembler = Assembler(name=assembler_name, version=assembler_version).save(using=self.database)
            assembler = assembler.save()

        job = AssemblyJob(assembler=assembler, status=status, input_size=total_size, priority=priority)
        job.save(using=self.database)
        RunAssemblyJob(assembly_job=job, run=run).save(using=self.database)
        return job

    def save_assembly_job(self, run, total_size, assembler_name, assembler_version, status, priority=0):
        job = self.is_assembly_job_in_backlog(run.primary_accession, assembler_name, assembler_version)
        if job:
            job.status = status
            job.priority = max(priority, job.priority)
            job.save()
        else:
            logging.info('Creating new assembly job for run {}'.format(run.primary_accession))
            job = self.create_assembly_job(run, total_size, assembler_name, assembler_version, status, priority)
        return job

    def set_assemblyjobs_running(self, ena_handler, secondary_study_accession, run, assembler_name, assembler_version):
        study = self.save_study(ena_handler, secondary_study_accession)
        run_obj = self.save_run(study, run)
        status = AssemblyJobStatus.objects.using(self.database).get(description='running')
        self.save_assembly_job(run_obj, run['raw_data_size'], assembler_name, assembler_version, status)

    def set_assembly_job_pending(self, ena_handler, secondary_study_accession, run, assembler_name, assembler_version, priority):
        if not assembler_version:
            assembler_version =  self.get_latest_assembler_version(assembler_name)
        study = self.save_study(ena_handler, secondary_study_accession)
        run_obj = self.save_run(study, run)
        status = AssemblyJobStatus.objects.using(self.database).get(description='pending')
        self.save_assembly_job(run_obj, run['raw_data_size'], assembler_name, assembler_version, status, priority)

    def filter_active_runs(self, runs, args):
        return list(filter(lambda r: not self.is_assembly_job_in_backlog(r['run_accession'], args), runs))

    def get_latest_assembler_version(self, assembler_name):
        return Assembler.objects.using(self.database).filter(name=assembler_name).order_by('-version')[0].version

def sanitise_string(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def get_date(data, field):
    try:
        date = datetime.strptime(data[field], "%Y-%m-%d").date()
    except (ValueError, KeyError):
        date = datetime.now().date()
    return date
