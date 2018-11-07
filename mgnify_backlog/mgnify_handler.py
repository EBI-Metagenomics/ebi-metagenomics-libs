#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2017 EMBL - European Bioinformatics Institute
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

import django.db
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Max

os.environ['DJANGO_SETTINGS_MODULE'] = 'backlog_cli.settings'

django.setup()

from backlog.models import Study, Run, AssemblyJob, Assembler, AssemblyJobStatus, RunAssemblyJob, Biome, User, Pipeline, \
    UserRequest, AnnotationJobStatus, Assembly, AnnotationJob, AssemblyAnnotationJob, RunAnnotationJob


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
                compressed_data_size=run['raw_data_size'] if 'raw_data_size' in run else 0,
                sample_primary_accession=run[
                    'secondary_sample_accession'] if 'secondary_sample_accession' in run else None,
                biome_validated=False
                )
        biome = Biome.objects.using(self.database).get(lineage=run['lineage'])
        r.biome = biome
        r.clean_fields()
        r.save(using=self.database)
        return r

    # def get_assemblies(database, secondary_study_accession, run_accessions, assembler, version):
    #     return Run.objects.using(database).filter()

    def get_backlog_study(self, primary_accession):
        return Study.objects.using(self.database).get(primary_accession=primary_accession)

    def get_backlog_secondary_study(self, secondary_study_accession):
        return Study.objects.using(self.database).get(secondary_accession=secondary_study_accession)

    def get_backlog_run(self, run_accession):
        return Run.objects.using(self.database).get(primary_accession=run_accession)

    def get_or_save_study(self, ena_handler, secondary_study_accession):
        try:
            return self.get_backlog_study(secondary_study_accession)
        except ObjectDoesNotExist:
            study = ena_handler.get_study(secondary_study_accession)
            return self.create_study_obj(study)

    def get_or_save_run(self, study, run, lineage):
        try:
            return self.get_backlog_run(run['run_accession'])
        except ObjectDoesNotExist:
            if not lineage and not 'lineage' in run:
                raise ValueError('Lineage not provided, cannot create new run')
            else:
                run['lineage'] = lineage
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

    def get_user(self, webin_id):
        return User.objects.using(self.database).get(webin_id=webin_id)

    def create_user(self, webin_id, email, first_name, surname, registered=False, consent_given=False):
        user = User(webin_id=webin_id, email_address=email, first_name=first_name, surname=surname,
                    registered=registered, consent_given=consent_given)
        user.save(using=self.database)
        return user

    def create_user_request(self, user, priority, rt_ticket):
        request = UserRequest(webin_id=user, priority=priority, rt_ticket=rt_ticket)
        request.save(using=self.database)
        return request

    def get_user_request(self, rt_ticket):
        return UserRequest.objects.using(self.database).get(rt_ticket=rt_ticket)

    def get_latest_pipeline(self):
        return Pipeline.objects.using(self.database).order_by('-version').first()

    def create_annotation_job(self, request, assembly_or_run, priority):
        latest_pipeline = self.get_latest_pipeline()
        status = AnnotationJobStatus.objects.using(self.database).get(description='SCHEDULED')
        job = AnnotationJob(request=request, pipeline=latest_pipeline, priority=priority)
        job.exec_status_id = status.id
        job.save(using=self.database)

        if type(assembly_or_run) is Run:
            run_annotation_job = RunAnnotationJob(run=assembly_or_run, annotation_job=job)
            run_annotation_job.save(using=self.database)
        elif type(assembly_or_run) is Assembly:
            assembly_annotation_job = AssemblyAnnotationJob(assembly=assembly_or_run, annotation_job=job)
            assembly_annotation_job.save(using=self.database)

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
            job.priority = max(priority, job.priority or 0)
            job.save()
        else:
            logging.info('Creating new assembly job for run {}'.format(run.primary_accession))
            job = self.create_assembly_job(run, total_size, assembler_name, assembler_version, status, priority)
        return job

    def set_assemblyjobs_running(self, ena_handler, secondary_study_accession, run, assembler_name, assembler_version):
        study = self.get_or_save_study(ena_handler, secondary_study_accession)
        run_obj = self.get_or_save_run(study, run)
        status = AssemblyJobStatus.objects.using(self.database).get(description='running')
        self.save_assembly_job(run_obj, run['raw_data_size'], assembler_name, assembler_version, status)

    def set_assembly_job_pending(self, ena_handler, secondary_study_accession, run, assembler_name, assembler_version,
                                 priority):
        if not assembler_version:
            assembler_version = self.get_latest_assembler_version(assembler_name)
        study = self.get_or_save_study(ena_handler, secondary_study_accession)
        run_obj = self.get_or_save_run(study, run)
        status = AssemblyJobStatus.objects.using(self.database).get(description='pending')
        self.save_assembly_job(run_obj, run['raw_data_size'], assembler_name, assembler_version, status, priority)

    def filter_active_runs(self, runs, args):
        return list(filter(lambda r: not self.is_assembly_job_in_backlog(r['run_accession'], args), runs))

    def get_latest_assembler_version(self, assembler_name):
        return Assembler.objects.using(self.database).filter(name=assembler_name).order_by('-version')[0].version

    def get_pending_assembly_jobs(self):
        return AssemblyJob.objects.using(self.database).filter(status=1).order_by('-priority')

    def is_valid_lineage(self, lineage):
        return len(Biome.objects.using(self.database).filter(lineage=lineage)) > 0

    # Get a list of runs in study which have been annotated with latest pipeline
    def get_up_to_date_annotation_jobs(self, study_accession):
        latest_pipeline = self.get_latest_pipeline()
        return Run.objects.using(self.database).filter(study__secondary_accession=study_accession,
                                                       runannotationjob__annotation_job__pipeline=latest_pipeline)


def sanitise_string(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])


def get_date(data, field):
    try:
        date = datetime.strptime(data[field], "%Y-%m-%d").date()
    except (ValueError, KeyError):
        date = datetime.now().date()
    return date
