import argparse
import csv
import logging
import re
import sys
from enum import Enum


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Tool to create decorate embl flatfile.')
    parser.add_argument('input_file',
                        help='tbl formatted deoverlapped output file')
    parser.add_argument('--rfam_lookup_file',
                        help='tsv formatted file mapping RFAMs accessions to name, description, '
                             'RNA type and model length',
                        default='rfam_family_lookup.tsv')
    parser.add_argument('-o', '--output-file', help='report')
    parser.add_argument('-v', '--verbose', action='store_true')
    return parser.parse_args(args)


class RNAType(Enum):
    R_RNA = 'rRNA'
    T_RNA = 'tRNA'
    M_RNA = 'mRNA'
    TM_RNA = 'tmRNA'
    NC_RNA = 'ncRNA'
    MISC_RNA = 'misc_RNA'
    SN_RNA = 'snRNA'
    SRP_RNA = 'ncRNA'


UNRESOLVED_RFAM_LOOKUP = {
    'RF01849': RNAType.TM_RNA,
    'RF01854': RNAType.SRP_RNA,
    'RF01850': RNAType.TM_RNA,
    'RF00017': RNAType.SRP_RNA,
    'RF01855': RNAType.SRP_RNA,
}


class RfamEntry:
    """
        Describes an RFAM entry.
        e.g.
        RF00001	5S_rRNA	5S ribosomal RNA	Gene; rRNA;	119
    """

    def __init__(self, accession, name, desc, rna_type, model_length, nc_rna_class=None):
        self.accession = accession
        self.name = name
        self.desc = desc
        self.rna_type = rna_type
        self.model_length = model_length
        self.nc_rna_class = nc_rna_class


class Inference:
    """
        Describes which prediction tool were used and which entry (including the database) was assigned.

        Used to populate flat file feature table.

        Example:
            /inference="similar to RNA sequence, rRNA:RFAM:RF00002"
            /inference="ab initio prediction:Infernal cmsearch:1.1.2"

            or

            /inference="protein motif:InterPro:IPR013766"
            /inference="ab initio prediction:InterProScan:5.32-71.0"

    """

    def __init__(self, prediction, software):
        self.prediction = prediction
        self.software = software


class WebinFeature:
    """
    Example:
        FT  rRNA       c+1..d
        FT             /gene="5.8S rRNA"
        FT             /product="5.8S ribosomal RNA"
        FT             /inference="similar to RNA sequence, rRNA:RFAM:RF02541"

        FT  <feature>       <seq_from>..<seq_to>
        FT             /gene="5.8S rRNA"
        FT             /product="5.8S ribosomal RNA"
        FT             /inference="similar to RNA sequence, rRNA:RFAM:RF02541"

    """

    def __init__(self, feature, start_pos, end_pos, gene, product, inference, start_complete=True, end_complete=True,
                 complement=False):
        """

        :param feature: Webin feature name, e.g. rRNA or ncRNA.
        :type feature: str
        :param start_pos: Feature start position.
        :type start_pos: int
        :param end_pos: Feature end position.
        :type end_pos: int
        :param gene: Feature name e.g. 5_8S_rRNA
        :type gene: str
        :param product: Feature description e.g. 5.8S ribosomal RNA
        :type product: str
        :param inference: Describes the inference line.
        :type inference: Inference
        :param start_complete: Default True, specify False for incomplete/unknown starts.
        :type start_complete: bool
        :param end_complete: Default True, specify False for incomplete/unknown ends.
        :type end_complete: bool
        :param complement: Default False, specify True for reverse strand features. If there are loci on both strands,
                           you will need to apply the complement operator to the coordinates for reverse strand
                           features.
        :type complement: bool
        """
        self.feature = feature
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.gene = gene
        self.product = product
        self.inference = inference
        self.start_complete = start_complete
        self.end_complete = end_complete
        self.complement = complement


class CMSearchMatch:
    """
        #target name         accession query name           accession mdl mdl from   mdl to seq from   seq to strand trunc pass   gc  bias  score   E-value inc description of target
    """

    def __init__(self, target_name, query_name, accession, model, model_from, model_to, seq_from, seq_to, forward):
        self.target_name = target_name
        self.query_name = query_name
        self.accession = accession
        self.model = model
        self.model_from = model_from
        self.model_to = model_to
        self.seq_from = seq_from
        self.seq_to = seq_to
        self.forward = forward


def parse_matches(input_file):
    matches = []
    with open(input_file, "r") as fp:
        cnt = 0
        for line in fp:
            line = line.rstrip("\n\r")
            chunks = re.findall(r'(\S+)', line)
            if len(chunks) != 18:
                raise ValueError("Unexpected number of chunks: {}".format(len(chunks)))
            forward = True if chunks[9] == '+' else False
            match = CMSearchMatch(chunks[0], chunks[2], chunks[3], chunks[4], int(chunks[5]), int(chunks[6]),
                                  int(chunks[7]), int(chunks[8]), forward)
            matches.append(match)
            cnt += 1
        logging.info("Processed {} matches.".format(cnt))
    return matches


def __process_rna_type(rfam_name, rna_type):
    """
        Possible RNA type values are:
            - Gene;
            - Gene; rRNA;
            - Gene; snRNA; splicing;
            - Cis-reg; riboswitch;

        Ribosomal RNA will go in as rRNA and transfer RNA will go in as tRNA. All remaining will go in as ncRNA.

        Controlled vocabulary for ncRNA classes:
        http://www.insdc.org/documents/ncrna-vocabulary

    :param param:
    :return:
    """
    non_coding_rna_class = None
    rna_type_result = None
    if 'tRNA' in rna_type:
        rna_type_result = RNAType.T_RNA
    elif 'rRNA' in rna_type:
        rna_type_result = RNAType.R_RNA
    else:
        rna_type_result = RNAType.NC_RNA
        if 'antisense' in rna_type:
            non_coding_rna_class = 'antisense_RNA'
        elif rna_type.startswith('Intron;'):
            non_coding_rna_class = 'autocatalytically_spliced_intron'
        elif 'ribozyme' in rna_type:
            if 'Hammerhead' in rfam_name:
                non_coding_rna_class = 'hammerhead_ribozyme'
            elif 'RNase_MRP' in rfam_name:
                non_coding_rna_class = 'RNase_MRP_RNA'
            else:
                non_coding_rna_class = 'ribozyme'
        elif 'lncRNA' in rna_type:
            non_coding_rna_class = 'lncRNA'
        elif 'RNase' in rfam_name:
            non_coding_rna_class = 'RNase_P_RNA'
        elif 'Telomerase' in rfam_name:
            non_coding_rna_class = 'telomerase_RNA'
        elif 'miRNA' in rna_type:
            non_coding_rna_class = 'miRNA'
        elif rna_type.startswith('Gene; snRNA'):
            non_coding_rna_class = 'snRNA'
        elif 'Vault' in rfam_name:
            non_coding_rna_class = 'vault_RNA'
        elif 'Y_RNA' in rfam_name:
            non_coding_rna_class = 'Y_RNA'
        elif '_SRP' in rfam_name:
            non_coding_rna_class = 'SRP_RNA'
        else:
            non_coding_rna_class = 'other'

    return rna_type_result, non_coding_rna_class


def parse_rfam_lookup_file(input_file):
    """
        Maps RFAMs accessions to name, description, RNA type and model length

    :param input_file:
    :return:
    """
    rfam_lookup = {}
    with open(input_file, "r") as tsv_file:
        reader = csv.reader(tsv_file, delimiter='\t')
        cnt = 0
        for row in reader:
            if len(row) != 5:
                raise ValueError("Unexpected number of chunks: {}".format(len(row)))
            accession = row[0]
            name = row[1]
            desc = row[2]
            rna_type, non_coding_rna_class = __process_rna_type(name, row[3])
            clength = row[4]
            new_entry = RfamEntry(accession, name, desc, rna_type, clength, non_coding_rna_class)
            rfam_lookup[accession] = new_entry
            cnt += 1
        logging.info("Processed {} models.".format(cnt))
    return rfam_lookup


def calculate_model_coverage(matches, model_lengths):
    print("<=== Model coverage approach ===>")
    partial = 0
    complete = 0
    for match in matches:
        rfam_accession = match.accession
        model_length = model_lengths.get(rfam_accession)
        coverage = (match.model_to - match.model_from) / model_length
        if coverage >= 0.9:
            complete += 1
        else:
            partial += 1

            # print("{}: {}%".format(match.target_name, coverage))

    print("Complete matches: {}".format(complete))
    print("Partial matches: {}".format(partial))


def calculate_missing_n(matches, model_lengths):
    print("<=== Missing N approach ===>")
    partial = 0
    complete = 0
    for match in matches:
        rfam_accession = match.accession
        model_length = model_lengths.get(rfam_accession)
        left_end_ok = match.model_from < 6
        right_end_ok = model_length - match.model_to < 6
        if left_end_ok and right_end_ok:
            complete += 1
        else:
            partial += 1

    print("Complete matches: {}".format(complete))
    print("Partial matches: {}".format(partial))


def create_webin_feature(match, model_lengths):
    rfam_accession = match.accession
    model_length = model_lengths.get(rfam_accession)
    #
    feature = ""  # feature needs to be look up from a dictionary
    start_pos = match.seq_from if match.forward else match.seq_to
    end_pos = match.seq_to if match.forward else match.seq_from
    gene = match.query_name
    product = ""  # feature needs to be look up from a dictionary
    inference_prediction = "similar to RNA sequence, rRNA:RFAM:{}".format(rfam_accession)
    inference = Inference(inference_prediction, "ab initio prediction:Infernal cmsearch:1.1.2")
    start_complete = True if match.model_from < 6 else False
    end_complete = True if model_length - match.model_to < 6 else False
    complement = True if not match.forward else False
    new_feature = WebinFeature(feature, start_pos, end_pos, gene, product, inference, start_complete, end_complete,
                               complement)
    return new_feature


def build_rfam_lookup(infile):
    return parse_rfam_lookup_file(infile)


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    rfam_dict = build_rfam_lookup(args.rfam_lookup_file)

    sequence_feature_dict = {}  # seq -> set of features
    for match in parse_matches(args.input_file):
        seq_id = match.target_name
        if seq_id in sequence_feature_dict:
            logging.info("Sequence {} has already a feature assigned.".format(seq_id))
            features = sequence_feature_dict.get(seq_id)
        else:
            features = set()
            sequence_feature_dict[seq_id] = features

        new_feature = create_webin_feature(match, model_lengths)
        features.add(new_feature)

    print()
    # TODO: Continue


if __name__ == '__main__':
    main(sys.argv[1:])
