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

import re

primary_study_accession_re = re.compile('(PRJ(EB|DB|NA|DA)\d+)')
secondary_study_accession_re = re.compile('([ESD]RP\d{5,})')
run_accession_re = re.compile('([ESD]RR\d{5,})')


def is_ena_study_accession(accession):
    return is_primary_study_acc(accession) or is_secondary_study_acc(accession)


def is_primary_study_acc(accession):
    return primary_study_accession_re.match(accession)


def is_secondary_study_acc(accession):
    return secondary_study_accession_re.match(accession)


def is_run_accession(accession):
    return run_accession_re.match(accession)
