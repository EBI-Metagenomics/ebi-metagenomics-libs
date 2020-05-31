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
import re

from mgnify_util.parser.interproscan_parser import InterProScanTSVResultParser
from ena.flatfile_decorator.prototype_rna_webin_feature_builder import RNA

__author__ = "Maxim Scheremetjew"
__version__ = "0.1"
__status__ = "Development"


def parse_accession(aline):
    return aline.replace('AC * _', '').rstrip() # get contig ID i.e. Trinity...


def get_coding_index(block: list):
    for item in block:
        if 'CDS' in item:
            coding_index = block.index(item)
            return coding_index + 1


def get_rna_index(block: list):
    for item in block:
        if 'SQ   Sequence' in item:
            rna_index = block.index(item)
            return rna_index - 1


def count_features(block: list):
    count = 0
    for item in block:
        if 'FT' in item:
            count += 1
    return count


def add_rna_annotations(accession: str, rna_dict: dict, block: list, tag_number: int):
    rna_index = get_rna_index(block)
    if accession in rna_dict:
        rna_list = rna_dict[accession]
        new_locus = [i+str(tag_number) if '/locus_tag' in i else i for i in rna_list]
        print(new_locus)
        block.insert(rna_index, '\n'.join(new_locus) + '\n')
    return block


class FlatfileDecorator:

    def __init__(self, input_embl_flatfile, output_file, rna_file, rna_lookup_file): #set input and output file
        self._input_embl_flatfile = open(input_embl_flatfile, 'r')
        self._output_file = output_file
        self.rna_file = rna_file
        self.rna_lookup_file = rna_lookup_file


    @staticmethod
    def lookup_seq_id(seq_id: str, annotations: dict): #get the sequence identifier from annotation dict - what is p1 and p2???
        # Add peptide extension
        identifier = f'{seq_id}.p1'
        if identifier not in annotations:
            identifier = f'{seq_id}.p2'
            if identifier not in annotations:
                return None
        return annotations.get(identifier)

    def get_block(self):
        infile = self._input_embl_flatfile  #set input and output files
        all_list = []
        block_list = []
        for aline in infile:
            if aline.startswith('ID'):
                block_list = [aline]
            elif aline.startswith('//'):
                block_list.append(aline)
                all_list.append(block_list)
            else:
                block_list.append(aline)
        return all_list

    def add_func_annotations(self, annotation_map: dict,
                             i5_version: str,
                             accession: str, block: list):
        """
            This method call will perform the actual decoration with functional
            annotations.

            List of databases can be found here:
            https://www.ncbi.nlm.nih.gov/genbank/collab/db_xref/
        :return:
        """
        acc = accession
        coding_index = get_coding_index(block)
        annotations = self.lookup_seq_id(acc, annotation_map)  # get any annotations with ID
        annot_count = 0
        prefix = 'FT                   '
        if annotations:
            for annot in annotations.get_all_annotations():
                database = annot.database
                identifier = annot.identifier
                if coding_index:
                    block.insert(coding_index, f'{prefix}/inference="protein motif:{database}:{identifier}"\n')
                    annot_count += 1
            block.insert(coding_index + annot_count, f'{prefix}/inference="ab initio prediction:InterProScan:{i5_version}"\n')
        return block


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Tool to create decorate embl flatfile.')
    parser.add_argument('in_flatfile', help='EMBL flatfile to decorate')
    parser.add_argument('i5_annotation_file',
                        help='TSV formatted I5 annotation file')
    parser.add_argument('rna_file', help='RNA deoverlapped annotations')
    parser.add_argument('rna_lookup_file')
    parser.add_argument('i5_version', help='Version of InterProScan used at the time of annotation calculation',
                        type=str)
    parser.add_argument('--tag-name', help='Name of the tag to search for in the CDS feature section.',
                        default='transl_table')
    parser.add_argument('-o', '--out_flatfile', help='EMBL flatfile output')
    parser.add_argument('-v', '--verbose', action='store_true')
    return parser.parse_args(args)


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    input_file = args.in_flatfile
    annotation_file = args.i5_annotation_file
    i5_version = args.i5_version
    tag_name = args.tag_name
    rna_file = args.rna_file
    rna_lookup_file = args.rna_lookup_file
    accession = ''

    if not os.path.exists(annotation_file):
        logging.ERROR(f'File {annotation_file} does not exist!')
        sys.exit(1)

    if os.path.exists(input_file) and not args.out_flatfile:
        dir_name, file_name = os.path.split(input_file)
        output_file_name = file_name.replace('.embl', '.new.embl')
        output_file = open(os.path.join(dir_name, output_file_name), 'w')
    else:
        logging.ERROR(f'File {input_file} does not exist!')
        sys.exit(1)

    ipro_parser = InterProScanTSVResultParser(annotation_file)
    ipro_parser.parse_file()

   rna_dict = RNA(rna_lookup_file, rna_file, tag_name)
    #get annotation blocks
    flatfile_decorator = FlatfileDecorator(input_file, output_file, rna_file, rna_lookup_file)
    all_blocks = flatfile_decorator.get_block()
    locus_prefix = 'FT                   /locus_tag='
    locus_tag = 1
    for x in all_blocks:
        parse_block = x
        for line in x:
            if 'AC *' in line:
                accession = parse_accession(line)
        block_with_func = flatfile_decorator.add_func_annotations(ipro_parser.annotations, i5_version, accession, parse_block)
        final_block_with_func = []
        for item in block_with_func:
            item = re.sub(r'FT\s+/locus_tag=.*', f'{locus_prefix}"{tag_name}_LOCUS{str(locus_tag)}"', item)
            final_block_with_func.append(item)
        block_with_rna = add_rna_annotations(accession, rna_dict, final_block_with_func, locus_tag)
        final_block = []
        if count_features(block_with_rna) > 3:
            for item in block_with_rna:
                item = re.sub(r'OS {3}.*\n|OC {3}.*\n|PR {3}.*\n', '', item)
                #item = re.sub(r'PR {3}.*', 'PR   Project:XXX;', item)
                final_block.append(item)
            output_file.write(''.join(final_block))
            locus_tag += 1
        else:
            final_block = block_with_rna
            output_file.write(''.join(block_with_rna))

    #output_file.close()
    #input_file.close()

if __name__ == '__main__':
    main(sys.argv[1:])
