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
import logging
import os
import sys

from interproscan_parser import InterProScanTSVResultParser

__author__ = "Maxim Scheremetjew"
__version__ = "0.1"
__status__ = "Development"


def parse_accession(aline):
    return aline.replace('AC * _', '').rstrip()


class FlatfileDecorator:

    def __init__(self, input_embl_flatfile, output_file):
        self._input_embl_flatfile = input_embl_flatfile
        self._output_file = output_file

    @staticmethod
    def lookup_seq_id(seq_id: str, annotations: dict):
        # Add peptide extension
        identifier = f'{seq_id}.p1'
        if identifier not in annotations:
            identifier = f'{seq_id}.p2'
            if identifier not in annotations:
                return None
        return annotations.get(identifier)

    def add_func_annotations(self, annotation_map: dict,
                             i5_version: str):
        """
            This method call will perform the actual decoration with functional
            annotations.

            List of databases can be found here:
            https://www.ncbi.nlm.nih.gov/genbank/collab/db_xref/
        :return:
        """
        infile = open(self._input_embl_flatfile, "r")
        outfile = open(self._output_file, "w")
        aline = infile.readline()
        while aline:
            if 'AC *' in aline:
                acc = parse_accession(aline)
                annotations = self.lookup_seq_id(acc, annotation_map)
            if "transl_table" in aline:
                index = aline.index('transl_table')
                new_line_start = aline[0:index - 1]
                if annotations:
                    new_lines = [aline]
                    for annot in annotations.get_all_annotations():
                        database = annot.database
                        identifier = annot.identifier
                        new_line = ''.join(
                            [new_line_start,
                             f'/inference="protein motif:{database}:{identifier}"\n'])
                        new_lines.append(new_line)
                    # Add prediction tool line
                    new_lines.append(''.join(
                        [new_line_start,
                         f'/inference="ab initio prediction:InterProScan:{i5_version}"\n']))

                    outfile.writelines(new_lines)
            else:
                outfile.write(aline)

            aline = infile.readline()

        infile.close()
        outfile.close()


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Tool to create decorate embl flatfile.')
    parser.add_argument('in_flatfile', help='EMBL flatfile to decorate')
    parser.add_argument('i5_annotation_file',
                        help='TSV formatted I5 annotation file')
    parser.add_argument('i5_version', help='Version of InterProScan used at the time of annotation calculation',
                        type=str)
    parser.add_argument('-o', '--out_flatfile', help='EMBL flatfile output')
    parser.add_argument('-v', '--verbose', action='store_true')
    return parser.parse_args(args)


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    input_file = args.in_flatfile
    annotation_file = args.i5_annotation_file
    i5_version = args.i5_version

    if not os.path.exists(annotation_file):
        logging.ERROR(f'File {annotation_file} does not exist!')
        sys.exit(1)

    if os.path.exists(input_file) and not args.out_flatfile:
        dir_name, file_name = os.path.split(input_file)
        output_file_name = file_name.replace('.embl', '.new.embl')
        output_file = os.path.join(dir_name, output_file_name)
    else:
        logging.ERROR(f'File {input_file} does not exist!')
        sys.exit(1)

    # Step 1: Parse InterProScan annotation file
    ipro_parser = InterProScanTSVResultParser(annotation_file)
    ipro_parser.parse_file()
    for seq_id, annotations in ipro_parser.annotations.items():
        logging.debug(seq_id)

    # TODO: Factor this out into another script
    # Step 2: Parse cmsearch matches
    # ipro_parser = InterProScanTSVResultParser(annotation_file)
    # ipro_parser.parse_file()
    # for seq_id, annotations in ipro_parser.annotations.items():
    #     print(seq_id)

    # Step 2: Decorate flaffile with db_xrefs
    flatfile_decorator = FlatfileDecorator(input_file, output_file)

    flatfile_decorator.add_func_annotations(ipro_parser.annotations, i5_version)

    # TODO: Factor this out into another script
    # flatfile_decorator.add_rna_annotations(ipro_parser.annotations)


if __name__ == '__main__':
    main(sys.argv[1:])
