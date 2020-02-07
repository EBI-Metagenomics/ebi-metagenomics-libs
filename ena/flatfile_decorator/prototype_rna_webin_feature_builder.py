import argparse
import logging
import re
import sys


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Tool to create decorate embl flatfile.')
    parser.add_argument('input_file',
                        help='tbl formatted deoverlapped output file')
    parser.add_argument('model_lengths',
                        help='tsv formatted file mapping rfams to model length')
    parser.add_argument('-o', '--output-file', help='report')
    parser.add_argument('-v', '--verbose', action='store_true')
    return parser.parse_args(args)


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


def parse_model_lengths(input_file):
    model_lengths = {}
    with open(input_file, "r") as fp:
        cnt = 0
        for line in fp:
            line = line.rstrip("\n\r")
            chunks = re.findall(r'(\S+)', line)
            if len(chunks) != 2:
                raise ValueError("Unexpected number of chunks: {}".format(len(chunks)))
            model_lengths[chunks[0]] = int(chunks[1])
            cnt += 1
        logging.info("Processed {} models.".format(cnt))
    return model_lengths


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


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    model_lengths = parse_model_lengths(args.model_lengths)

    sequence_feature_dict = {}  # seq -> set of features
    for match in parse_matches(args.input_file):
        seq_id = match.target_name
        if seq_id in sequence_feature_dict:
            features = sequence_feature_dict.get(seq_id)
            create_webin_feature(match, model_lengths)
    # TODO: Continue


if __name__ == '__main__':
    main(sys.argv[1:])
