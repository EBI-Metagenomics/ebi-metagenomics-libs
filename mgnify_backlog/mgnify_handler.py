#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2018 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime
import os
import logging
import re

import django.db
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q

os.environ['DJANGO_SETTINGS_MODULE'] = 'backlog_cli.settings'

django.setup()

from backlog.models import Study, Run, AssemblyJob, RunAssembly, Assembler, AssemblyJobStatus, RunAssemblyJob, Biome, \
    User, Pipeline, \
    UserRequest, AnnotationJobStatus, Assembly, AnnotationJob, AssemblyAnnotationJob, RunAnnotationJob

run_reg = r'([E|S|D]RR\d{5,})_?'


def summarise_description(description):
    fmt_description = sanitise_string(description)
    max_desc_length = Study._meta.get_field('description').max_length
    if len(fmt_description) > max_desc_length:
        fmt_description = fmt_description[0:max_desc_length - 5] + '...'
    return fmt_description


class MgnifyHandler:
    def __init__(self, database):
        self.database = database

    def set_biome(self, obj_data, obj):
        if 'inferred_lineage' in obj_data:
            biome = Biome.objects.using(self.database).get(lineage=obj_data['inferred_lineage'])
            obj.inferred_biome = biome
        if 'lineage' in obj_data:
            biome = Biome.objects.using(self.database).get(lineage=obj_data['lineage'])
            obj.biome = biome

    def create_study_obj(self, data):
        fmt_description = summarise_description(data.get('description'))

        s = Study(primary_accession=data['study_accession'],
                  secondary_accession=data['secondary_study_accession'],
                  title=sanitise_string(data['study_title']),
                  scientific_name=sanitise_string(data.get('scientific_name')),
                  description=fmt_description,
                  public=get_date(data, 'first_public') <= datetime.now().date(),
                  ena_last_update=get_date(data, 'last_updated')
                  )
        s.save(using=self.database)
        return s

    def update_study_obj(self, data):
        s = Study.objects.using(self.database).get(primary_accession=data['study_accession'],
                                                   secondary_accession=data['secondary_study_accession'])
        s.public = get_date(data, 'first_public') <= datetime.now().date()
        if 'last_updated' in data:
            s.ena_last_update = get_date(data, 'last_updated')

        for attr in ['study_title', 'scientific_name']:
            if attr in data:
                setattr(s, attr, sanitise_string(data[attr]))

        if 'description' in data:
            fmt_description = summarise_description(data.get('description'))
            setattr(s, 'description', fmt_description)
        s.save(using=self.database)

    def create_run_obj(self, study, run, public=True):
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
                compressed_data_size=run['raw_data_size'] if 'raw_data_size' in run else 0,
                sample_primary_accession=run[
                    'secondary_sample_accession'] if 'secondary_sample_accession' in run else None,
                public=public
                )
        self.set_biome(run, r)
        r.clean_fields()
        r.save(using=self.database)
        return r

    def update_run_obj(self, run_data):
        r = Run.objects.using(self.database).get(primary_accession=run_data['run_accession'])
        int_fields = ['base_count', 'read_count']
        [setattr(r, int_field, run_data[int_field]) for int_field in int_fields if int_field in run_data]
        string_fields = ['instrument_platform', 'instrument_model', 'instrument_strategy', 'instrument_']
        [setattr(r, string_field, sanitise_string(run_data[string_field])) for string_field in string_fields if
         string_field in run_data]
        if 'last_updated' in run_data:
            r.ena_last_update = run_data['last_updated']
        if 'raw_data_size' in run_data:
            r.compressed_data_size = run_data['raw_data_size']
        if 'secondary_sample_accession' in run_data:
            r.secondary_sample_accession = run_data['secondary_sample_accession']

        self.set_biome(run_data, r)

        r.clean_fields()
        r.save(using=self.database)
        r.save()
        return r

    def create_assembly_obj(self, ena_handler, study, assembly_data, public):

        assembly = Assembly(study=study,
                            primary_accession=assembly_data['analysis_accession'],
                            ena_last_update=assembly_data['last_updated'],
                            public=public)
        self.set_biome(assembly_data, assembly)
        assembly.save(using=self.database)
        alias = assembly_data['analysis_alias']
        if re.match(run_reg, alias):
            run_ids = re.findall(run_reg, assembly_data['analysis_alias'])
            for run_id in run_ids:
                run = self.get_or_save_run(ena_handler, run_id)
                RunAssembly(run=run, assembly=assembly).save()
        return assembly

    def update_assembly_obj(self, assembly_data):
        assembly = Assembly.objects.using(self.database).get(primary_accession=assembly_data['analysis_accession'])
        if 'last_updated' in assembly_data:
            assembly.ena_last_update = assembly_data['last_updated']
        assembly.clean_fields()
        assembly.save()
        if 'related_runs' in assembly_data:
            run_accessions = assembly.runassembly_set.all().values_list('run__primary_accession', flat=True)
            for run in assembly_data['related_runs']:
                if not isinstance(run, Run):
                    run = self.get_backlog_run(run)
                if run.primary_accession not in run_accessions:
                    RunAssembly(run=run, assembly=assembly).save()
        return assembly

    def get_backlog_study(self, primary_accession=None, secondary_accession=None):
        query = Study.objects.using(self.database)
        if primary_accession:
            query = query.filter(primary_accession=primary_accession)
        if secondary_accession:
            query = query.filter(secondary_accession=secondary_accession)
        if len(query) == 0:
            raise ObjectDoesNotExist('Study {} {} could not be found.'.format(primary_accession, secondary_accession))
        return query[0]

    def get_backlog_run(self, run_accession):
        return Run.objects.using(self.database).get(primary_accession=run_accession)

    def get_backlog_assembly(self, assembly_accession):
        return Assembly.objects.using(self.database).get(primary_accession=assembly_accession)

    def get_or_save_study(self, ena_handler, primary_accession=None, secondary_accession=None):
        try:
            return self.get_backlog_study(primary_accession, secondary_accession)
        except ObjectDoesNotExist:
            study = ena_handler.get_study(primary_accession=primary_accession,
                                          secondary_accession=secondary_accession)
            return self.create_study_obj(study)

    def get_or_save_run(self, ena_handler, run_accession, study=None,
                        lineage=None, public=True):
        try:
            return self.get_backlog_run(run_accession)
        except ObjectDoesNotExist:
            run = ena_handler.get_run(run_accession, public=public)
            if lineage:
                run['lineage'] = lineage
            if not study:
                study = self.get_or_save_study(ena_handler, run['study_accession'])
            return self.create_run_obj(study, run, public)

    def get_or_save_assembly(self, ena_handler, accession, assembly, study=None, public=True):
        try:
            return self.get_backlog_assembly(accession)
        except ObjectDoesNotExist:
            if not study:
                study = self.get_or_save_study(ena_handler, assembly['study_accession'])
            return self.create_assembly_obj(ena_handler, study, assembly, public)

    def is_assembly_job_in_backlog(self, primary_accession, assembler_name, assembler_version=None):
        if not assembler_version:
            jobs = AssemblyJob.objects.using(self.database) \
                .filter(runs__primary_accession=primary_accession,
                        assembler__name=assembler_name).order_by('-assembler__version')
        else:
            jobs = AssemblyJob.objects.using(self.database).filter(runs__primary_accession=primary_accession,
                                                                   assembler__name=assembler_name,
                                                                   assembler__version=assembler_version)
        return jobs[0] if len(jobs) > 0 else None

    def get_user(self, webin_id):
        return User.objects.using(self.database).get(webin_id=webin_id)

    def create_user(self, webin_id, email, first_name, surname, registered=False, consent_given=False):
        user = User(webin_id=webin_id, email_address=email, first_name=first_name, surname=surname,
                    registered=registered, consent_given=consent_given)
        user.save(using=self.database)
        return user

    def create_user_request(self, user, priority, rt_ticket):
        request = UserRequest(user=user, priority=priority, rt_ticket=rt_ticket)
        request.save(using=self.database)
        return request

    def get_user_request(self, rt_ticket):
        return UserRequest.objects.using(self.database).get(rt_ticket=rt_ticket)

    def get_latest_pipeline(self):
        return Pipeline.objects.using(self.database).order_by('-version').first()

    def get_pipeline_by_version(self, version):
        return Pipeline.objects.using(self.database).get(version=version)

    def create_annotation_job(self, request, assembly_or_run, priority, pipeline_version=None):
        """

        :param pipeline_version:
        :type pipeline_version: float
        :return:
        """
        if pipeline_version:
            pipeline = self.get_pipeline_by_version(pipeline_version)
        else:
            pipeline = self.get_latest_pipeline()
        status = AnnotationJobStatus.objects.using(self.database).get(description='SCHEDULED')
        job = AnnotationJob(request=request, pipeline=pipeline, priority=priority)
        job.status = status
        job.save(using=self.database)

        if isinstance(assembly_or_run, Run):
            run_annotation_job = RunAnnotationJob(run=assembly_or_run, annotation_job=job)
            run_annotation_job.save(using=self.database)
        elif isinstance(assembly_or_run, Assembly):
            assembly_annotation_job = AssemblyAnnotationJob(assembly=assembly_or_run, annotation_job=job)
            assembly_annotation_job.save(using=self.database)
        return job

    # Status can be AssemblyJobStatus or string description of status
    def create_assembly_job(self, run, total_size, status, assembler_name, assembler_version=None, priority=0):
        try:
            if not assembler_version:
                assembler_version = self.get_latest_assembler_version(assembler_name)
            assembler = Assembler.objects.using(self.database).get(name=assembler_name, version=assembler_version)
        except ObjectDoesNotExist:
            assembler = Assembler(name=assembler_name, version=assembler_version)
            assembler.save(using=self.database)

        if isinstance(status, str):
            status = AssemblyJobStatus.objects.using(self.database).get(description=status)
        job = AssemblyJob(assembler=assembler, status=status, input_size=total_size, priority=priority)
        job.save(using=self.database)
        RunAssemblyJob(assembly_job=job, run=run).save(using=self.database)
        return job

    def save_assembly_job(self, run, total_size, assembler_name, assembler_version, status, priority=0):
        job = self.is_assembly_job_in_backlog(run.primary_accession, assembler_name, assembler_version)
        if job:
            job.status = status
            job.priority = max(priority, job.priority or 0)
            job.save()
        else:
            logging.info('Creating new assembly job for run {}'.format(run.primary_accession))
            job = self.create_assembly_job(run, total_size, status, assembler_name, assembler_version, priority)
        return job

    def set_assembly_job_running(self, run_accession, assembler_name,
                                 assembler_version):
        status = AssemblyJobStatus.objects.using(self.database).get(description='running')
        jobs = AssemblyJob.objects.using(self.database).filter(runs__primary_accession=run_accession,
                                                               assembler__name=assembler_name,
                                                               assembler__version=assembler_version)
        for job in jobs:
            job.status = status
            job.save()

    def set_assembly_job_pending(self, run_accession, assembler_name,
                                 assembler_version):
        # if not assembler_version:
        #     assembler_version = self.get_latest_assembler_version(assembler_name)
        status = AssemblyJobStatus.objects.using(self.database).get(description='pending')
        jobs = AssemblyJob.objects.using(self.database).filter(runs__primary_accession=run_accession,
                                                               assembler__name=assembler_name,
                                                               assembler__version=assembler_version)
        for job in jobs:
            job.status = status
            job.save()

    def filter_active_runs(self, runs, assembler, version=None):
        return list(filter(lambda r: not self.is_assembly_job_in_backlog(r['run_accession'], assembler, version), runs))

    def get_latest_assembler_version(self, assembler_name):
        try:
            return Assembler.objects.using(self.database).filter(name=assembler_name).order_by('-version')[0].version
        except IndexError:
            raise ObjectDoesNotExist

    def get_pending_assembly_jobs(self):
        return AssemblyJob.objects.using(self.database).filter(status__description='pending').order_by('-priority')

    def is_valid_lineage(self, lineage):
        try:
            Biome.objects.using(self.database).get(lineage=lineage)
            return True
        except ObjectDoesNotExist:
            return False

    # Get a list of runs in study which have been annotated with latest pipeline
    def get_up_to_date_run_annotation_jobs(self, study_accession, pipeline_version=None):
        if pipeline_version:
            pipeline = self.get_pipeline_by_version(pipeline_version)
        else:
            pipeline = self.get_latest_pipeline()
        return Run.objects.using(self.database).filter(study__secondary_accession=study_accession,
                                                       runannotationjob__annotation_job__pipeline=pipeline)

    def get_up_to_date_assembly_annotation_jobs(self, study_accession, pipeline_version=None):
        if pipeline_version:
            pipeline = self.get_pipeline_by_version(pipeline_version)
        else:
            pipeline = self.get_latest_pipeline()

        return Assembly.objects.using(self.database).filter(study__secondary_accession=study_accession,
                                                            assemblyannotationjobs__annotation_job__pipeline=pipeline)

    def set_annotation_jobs_completed(self, study, rt_ticket, excluded_runs=None):
        if not excluded_runs:
            excluded_runs = []
        completed_status = AnnotationJobStatus.objects.using(self.database).get(description='COMPLETED')
        jobs = AnnotationJob.objects.using(self.database).filter(
            Q(assemblyannotationjob__assembly__study=study) |
            Q(runannotationjob__run__study=study), request__rt_ticket=rt_ticket).exclude(
            runannotationjob__run__primary_accession__in=excluded_runs).exclude(
            assemblyannotationjob__assembly__primary_accession__in=excluded_runs)
        jobs.update(status=completed_status)

    def set_annotation_jobs_failed(self, study, rt_ticket, failed_runs):
        failed_status = AnnotationJobStatus.objects.using(self.database).get(description='FAILED')
        jobs = AnnotationJob.objects.using(self.database).filter(
            Q(assemblyannotationjob__assembly__study=study) |
            Q(runannotationjob__run__study=study), request__rt_ticket=rt_ticket).filter(
            Q(runannotationjob__run__primary_accession__in=failed_runs) | Q(
                assemblyannotationjob__assembly__primary_accession__in=failed_runs))

        jobs.update(status=failed_status)

    def set_assembly_annotation_job_protein_db(self, assembly_accessions, value=True):
        jobs = AssemblyAnnotationJob.objects.using(self.database).filter(
            Q(assembly__primary_accession__in=assembly_accessions))
        jobs.update(protein_db=value)

    def get_request_webin(self, rt_ticket):
        return UserRequest.objects.using(self.database).get(rt_ticket=rt_ticket).user.webin_id

    def get_annotation_job_status(self, description):
        return AnnotationJobStatus.objects.using(self.database).get(description=description)

    def get_annotation_jobs(self, run_or_assembly_accessions=None, study_accessions=None, status_descriptions=None,
                            priority=None, pipeline_version=None, experiment_types=None, biome_assigned_only=False,
                            in_protein_db=None):
        jobs = AnnotationJob.objects.using(self.database)
        if run_or_assembly_accessions:
            jobs = jobs.filter(
                Q(runannotationjob__run__primary_accession__in=run_or_assembly_accessions) |
                Q(assemblyannotationjob__assembly__primary_accession__in=run_or_assembly_accessions))
        if study_accessions:
            jobs = jobs.filter(
                Q(runannotationjob__run__study__primary_accession__in=study_accessions) |
                Q(runannotationjob__run__study__secondary_accession__in=study_accessions) |
                Q(assemblyannotationjob__assembly__study__primary_accession__in=study_accessions) |
                Q(assemblyannotationjob__assembly__study__secondary_accession__in=study_accessions))
        if biome_assigned_only:
            jobs = jobs.filter(
                Q(runannotationjob__run__biome_id__isnull=False) |
                Q(assemblyannotationjob__assembly__biome_id__isnull=False))
        if in_protein_db is not None:
            # filter by field protein_db from AssemblyAnnotationJob.
            jobs = jobs.filter(Q(assemblyannotationjob__protein_db=in_protein_db))
        if experiment_types and len(experiment_types):
            q_objects = Q()
            no_assembly = list(filter(lambda exp: exp != 'ASSEMBLY', experiment_types))
            if len(no_assembly):
                q_objects |= Q(runannotationjob__run__library_strategy__in=experiment_types)
            if 'ASSEMBLY' in experiment_types:
                q_objects |= Q(assemblyannotationjob__isnull=False)
            jobs = jobs.filter(q_objects)

            if 'ASSEMBLY' in experiment_types:
                jobs = jobs.distinct()
        if priority:
            jobs = jobs.filter(priority=priority)
        if status_descriptions:
            if isinstance(status_descriptions, str):
                status_descriptions = [status_descriptions]
            jobs = jobs.filter(status__description__in=status_descriptions)
        if pipeline_version:
            jobs = jobs.filter(pipeline__version=pipeline_version)
        return jobs

    def update_annotation_jobs_status(self, annotation_jobs, status_description):
        try:
            status = self.get_annotation_job_status(status_description)
            annotation_jobs.update(status=status)
        except ObjectDoesNotExist:
            statuses = ','.join(AnnotationJobStatus.objects.using(self.database).values_list('description', flat=True))
            raise ValueError('Status {} is invalid. Valid choices are: {}'.format(status_description, statuses))

    def update_annotation_job(self, job, field_dict):
        for k, v in field_dict.items():
            setattr(job, k, v)
        job.save()

    def update_annotation_jobs_from_accessions(self, run_or_assembly_accessions=None, study_accessions=None,
                                               status_description=None, priority=None, pipeline_version=None,
                                               directory=None, delete=False, auto_confirm=False, result_status=None,
                                               library_strategy=None):
        jobs = self.get_annotation_jobs(run_or_assembly_accessions=run_or_assembly_accessions,
                                        study_accessions=study_accessions, pipeline_version=pipeline_version)

        logging.info('Matched {} annotation job(s)'.format(len(jobs)))

        if status_description:
            self.update_annotation_jobs_status(jobs, status_description)
            logging.info('Updated AnnotationJob status...')

        if result_status:
            jobs.update(result_status=result_status)
            logging.info('Updated AnnotationJob result_status...')

        if priority:
            jobs.update(priority=priority)
            logging.info('Updated AnnotationJob priority...')

        if directory and status_description == 'RUNNING':
            jobs.update(directory=directory)
            logging.info('Setting directory for launched jobs...')

        if delete:
            if auto_confirm or input('Please confirm you wish to delete {} jobs (yes/no): '.format(len(jobs))) == 'yes':
                jobs.delete()
                logging.info('Deleting annotation jobs')

        if library_strategy:
            runs = Run.objects.using(self.database).filter(annotationjobs__in=jobs)
            runs.update(library_strategy=library_strategy)
            # filtered out assembly accessions (no library strategy)
            logging.info('Updated library strategy for {} runs'.format(len(runs)))


def sanitise_string(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def get_date(data, field):
    try:
        date = datetime.strptime(data[field], "%Y-%m-%d").date()
    except (ValueError, KeyError):
        date = datetime.now().date()
    return date
