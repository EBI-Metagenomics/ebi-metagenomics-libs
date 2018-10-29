from datetime import datetime
import os

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
        print(run)
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

    def save_study(self, ena_handler, args):
        try:
            return self.is_study_in_backlog(args.study)
        except IndexError:
            study = ena_handler.get_study(args.study)
            return self.create_study_obj(study)

    def save_run(self, study, run):
        try:
            return self.is_run_in_backlog(run['run_accession'])
        except IndexError:
            return self.create_run_obj(study, run)

    def is_assembly_job_in_backlog(self, primary_accession, args):
        if args.ignore_version:
            jobs = AssemblyJob.objects.using(self.database).filter(runs__primary_accession=primary_accession,
                                                                   assembler__name=args.assembler)
        else:
            jobs = AssemblyJob.objects.using(self.database).filter(runs__primary_accession=primary_accession,
                                                                   assembler__name=args.assembler,
                                                                   assembler__version=args.assembler_version)
        return jobs[0] if len(jobs) > 0 else None

    def create_assembly_job(self, run, total_size, args, status):
        try:
            assembler = Assembler.objects.using(self.database).get(name=args.assembler, version=args.assembler_version)
        except ObjectDoesNotExist:
            assembler = Assembler(name=args.assembler, version=args.assembler_version).save(using=self.database)
            assembler = assembler.save()

        job = AssemblyJob(assembler=assembler, status=status, input_size=total_size)
        job.save(using=self.database)
        RunAssemblyJob(assembly_job=job, run=run).save(using=self.database)
        return job

    def save_assembly_job_running(self, run, total_size, args):
        job = self.is_assembly_job_in_backlog(run.primary_accession, args)
        status = AssemblyJobStatus.objects.using(self.database).get(description='running')
        if job:
            job.status = status
            job.save()
        else:
            job = self.create_assembly_job(run, total_size, args, status)
        return job

    def store_entries(self, ena_handler, runs, args):
        study = self.save_study(ena_handler, args)
        for run in runs:
            run_obj = self.save_run(study, run)
            total_size = get_raw_data_size(run)
            self.save_assembly_job_running(run_obj, total_size, args)

    def filter_active_runs(self, runs, args):
        return list(filter(lambda r: not self.is_assembly_job_in_backlog(r['run_accession'], args), runs))


def sanitise_string(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def get_date(data, field):
    try:
        date = datetime.strptime(data[field], "%Y-%m-%d").date()
    except (ValueError, KeyError):
        date = datetime.now().date()
    return date


def get_raw_data_size(run):
    return sum([os.path.getsize(f['location'].strip('file:')) for f in run['raw_reads']])
