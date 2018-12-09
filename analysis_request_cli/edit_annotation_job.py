import sys
import argparse
import logging

from mgnify_backlog import mgnify_handler


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description='Tool to change execution status and priority of annotation jobs on the mgnify backlog schema. If'
                    ' both studies and runs are provided, study annotation jobs will be filtered'
                    ' by the run accessions specified')
    parser.add_argument('-s', '--studies',
                        help='Comma separated list of run / assembly accessions (MGYS accessions not yet supported)')
    parser.add_argument('-ra', '--runs_and_assemblies', help='Comma separated list of run / assembly accessions')
    parser.add_argument('-ss', '--status', help='AnnotationJob status ')
    parser.add_argument('-p', '--priority', type=int, help='Priority to set for designated annotationJobs',
                        choices=range(0, 6))
    parser.add_argument('--pipeline_version', help='Pipeline version (defaults to latest analysis of the study/runs')
    parser.add_argument('--db', choices=['default', 'dev', 'prod'], default='default')
    parser.add_argument('-v', '--verbose')
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))

    if not args.studies and not args.runs_and_assemblies:
        logging.error('No study or runs specified, please use -s or -r')
        sys.exit(1)

    if not args.status and not args.priority:
        logging.error('No action specified, please use -ss or -p')
        sys.exit(1)

    mgnify = mgnify_handler.MgnifyHandler(args.db)

    if args.studies:
        args.studies = args.studies.split(',')
    if args.runs_and_assemblies:
        args.runs_and_assemblies = args.runs_and_assemblies.split(',')
    mgnify.update_annotation_jobs(args.runs_and_assemblies, args.studies, args.status, args.priority,
                                  args.pipeline_version)


if __name__ == '__main__':
    main(sys.argv[1:])
