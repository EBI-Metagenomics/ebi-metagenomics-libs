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
import argparse
import csv
import sys

__author__ = "Maxim Scheremetjew"
__version__ = "0.1"
__status__ = "Development"


class CMSearchMatch:
    """
        Describes a function annotation for instance InterPro:IPR004361 or GO:0004462.
    """

    def __init__(self, type, rfam_acc, description, start, end, forward_strand,
                 feature_coordinates):
        self.type = type
        self.rfam_acc = rfam_acc
        self.description = description
        self.start = start
        self.end = end
        self.forward_strand = forward_strand
        self.feature_coordinates = feature_coordinates

    def __hash__(self):
        return hash((self.rfam_acc, self.start, self.end))

    def __eq__(self, other):
        return self.rfam_acc == other.rfam_acc and self.start == other.start and self.end == other.end


class Annotations:
    """
        Describes a set function annotation which can be easily added by the add_annotation function.
    """

    def __init__(self):
        self._annotations = set()

    def add_annotation(self, type: str, rfam_acc: str, description: str,
                       start: int, end: int, strand: bool,
                       feature_coordinates: str):
        annotation = CMSearchMatch(type, rfam_acc, description, start, end,
                                   strand, feature_coordinates)
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


def remove_empty_elements(rows):
    results = []
    for item in rows:
        if item:
            results.append(item)
    return results


class DeoverlapResultParser:
    """
        Parses deoverlap input file and stores mappings between
        sequence accessions and cmsearch matches.
    """

    def __init__(self, input_file):
        self._input_file = input_file
        self._annotations = {}  # map of sequence accessions and matches

    def parse_file(self, rfam_parser=None):
        with open(self._input_file) as file:
            rows = csv.reader(file, delimiter=" ", quotechar='"')
            for row in rows:
                row = remove_empty_elements(row)
                if row[16] == '?':
                    continue
                seq_id = row[0]
                type = row[2]
                rfam_accession = row[3]
                forward_strand = True if row[9] == "+" else False
                start = row[7] if forward_strand else row[8]
                end = row[8] if forward_strand else row[7]

                description = None
                feature_coordinates = None
                if rfam_parser:
                    description = rfam_parser.get_description(rfam_accession)
                    feature_coordinates = rfam_parser.get_feature_coordinates(
                        rfam_accession, int(start), int(end), forward_strand)

                if seq_id not in self._annotations:
                    self._annotations[seq_id] = Annotations()
                else:
                    print(f'Found duplicate entry: {seq_id}')

                self._annotations.get(seq_id).add_annotation(type,
                                                             rfam_accession,
                                                             description,
                                                             start, end,
                                                             forward_strand,
                                                             feature_coordinates)


class RfamEntry:
    """
        Describes a Rfam entry.
    """

    def __init__(self, rfam_acc: str, description: str, clen: int):
        self.rfam_acc = rfam_acc
        self.description = description
        self.clen = clen  # model length

    def __hash__(self):
        return hash((self.rfam_acc, self.description, self.clen))

    def __eq__(self, other):
        return self.rfam_acc == other.rfam_acc and self.description == other.description and self.clen == other.clen


class RfamEntriesFileParser:
    """
        Parses the following CSV format file:

            "rfam_acc","description","clen"
            "RF02543","Eukaryotic large subunit ribosomal RNA",3401
            "RF02540","Archaeal large subunit ribosomal RNA",2990
            "RF02541","Bacterial large subunit ribosomal RNA",2925
            "RF02462","Ascomycota telomerase RNA",1859
    """

    def __init__(self, input_file):
        self._input_file = input_file
        self._rfam_entries = {}  # dict of Rfam entries

    def parse_file(self):
        with open(self._input_file) as file:
            # Skip header line
            next(file)
            rows = csv.reader(file, delimiter=",", quotechar='"')
            for row in rows:
                rfam_accession = row[0]
                description = row[1]
                clen = row[2]

                new_entry = RfamEntry(rfam_accession, description, int(clen))
                self._rfam_entries[rfam_accession] = new_entry

    def get_all_entries(self):
        return self._rfam_entries

    def get_description(self, accession: str):
        if accession in self._rfam_entries:
            rfam_entry = self._rfam_entries.get(accession)
            return rfam_entry.description
        return "n/a"

    def get_feature_coordinates(self, accession: str, start: int, end: int,
                                forward_strand: bool):
        """
            example: complement(216..>920)

        :param accession:
        :param start:
        :param end:
        :param forward_strand:
        :return:
        """
        if accession in self._rfam_entries:
            result = ""
            rfam_entry = self._rfam_entries.get(accession)
            clen = rfam_entry.clen
            if start > 10:
                result = f'{start}'
            if clen - end > 10:
                result = f'{end}'

            if forward_strand:
                return f'complement({result})'
            else:
                return result
        return "n/a"


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Tool to parse cmsearch deoverlap outputs')
    parser.add_argument('deoverlap_file',
                        help='CMSearch deoverlap outputs')
    parser.add_argument('rfam_entries',
                        help='List of Rfam entries')
    parser.add_argument('-v', '--verbose', action='store_true')
    return parser.parse_args(args)


def main(argv=None):
    args = parse_args(argv)
    deoverlap_file = args.deoverlap_file
    rfam_entries = args.rfam_entries
    rfam_parser = RfamEntriesFileParser(rfam_entries)
    rfam_parser.parse_file()
    #
    deoverlap_parser = DeoverlapResultParser(deoverlap_file)
    deoverlap_parser.parse_file(rfam_parser=rfam_parser)
    print()


if __name__ == '__main__':
    main(sys.argv[1:])
