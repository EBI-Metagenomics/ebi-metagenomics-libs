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
import csv

__author__ = "Maxim Scheremetjew"
__version__ = "0.1"
__status__ = "Development"


class FunctionalAnnotation:
    """
        Describes a function annotation for instance InterPro:IPR004361 or GO:0004462.
    """

    def __init__(self, database, identifier):
        self.database = database
        self.identifier = identifier

    def __hash__(self):
        return hash((self.database, self.identifier))

    def __eq__(self, other):
        return self.database == other.database and self.identifier == other.identifier


class Annotations:
    """
        Describes a set function annotation which can be easily added by the add_annotation function.
    """

    def __init__(self):
        self._annotations = set()

    def add_annotation(self, database: str, identifier: str):
        annotation = FunctionalAnnotation(database, identifier)
        self._annotations.add(annotation)

    @staticmethod
    def _get_identifiers(annotations: set) -> set:
        """
            Converts set of objects into a set of string.
        :param annotations:
        :return:
        """
        result = set()
        for annotation in annotations:
            result.add(annotation.identifier)
        return result

    def get_all_annotations(self):
        return self._annotations


class InterProScanTSVResultParser:
    """
        Parses TSV formatted input file and stores mappings between
        sequence accessions and functional annotations.
    """

    def __init__(self, input_tsv_file):
        self.input_tsv_file = input_tsv_file
        self.annotations = {}  # map of sequence accessions and functional annotations

    def parse_file(self):
        with open(self.input_tsv_file) as file:
            rows = csv.reader(file, delimiter="\t", quotechar='"')
            for row in rows:
                seq_id = row[0]
                if seq_id not in self.annotations:
                    self.annotations[seq_id] = Annotations()
                for x in range(11, len(row)):
                    if "IPR" in row[x]:
                        self.annotations.get(seq_id).add_annotation("InterPro",
                                                                    row[x])
                    elif "GO" in row[x]:
                        go_entries = row[x].split('|')
                        for go_entry in go_entries:
                            self.annotations.get(seq_id). \
                                add_annotation("GO",
                                               go_entry.replace('GO:', ''))
                    elif "KEGG" in row[x]:
                        pathway_entries = row[x].split('|')
                        for pathway_entry in pathway_entries:
                            if "KEGG" in pathway_entry:
                                self.annotations.get(seq_id).add_annotation(
                                    "KEGG",
                                    pathway_entry.replace('KEGG: ', ''))
                            elif "MetaCyc" in pathway_entry:
                                self.annotations.get(seq_id).add_annotation(
                                    "MetaCyc",
                                    pathway_entry.replace('MetaCyc: ', ''))
                            elif "Reactome" in pathway_entry:
                                self.annotations.get(seq_id).add_annotation(
                                    "Reactome",
                                    pathway_entry.replace('Reactome: ', ''))
