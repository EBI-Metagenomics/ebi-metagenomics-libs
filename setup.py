from setuptools import setup, find_packages

import os
import sys

version = "0.5.4"

_base = os.path.dirname(os.path.abspath(__file__))
_requirements = os.path.join(_base, 'requirements.txt')
_requirements_test = os.path.join(_base, 'requirements-test.txt')
_env_activate = os.path.join(_base, 'venv', 'bin', 'activate')

install_requirements = []
with open(_requirements) as f:
    install_requirements = f.read().splitlines()

test_requirements = []
if "test" in sys.argv:
    with open(_requirements_test) as f:
        test_requirements = f.read().splitlines()

setup(
    name="emg_libs",
    author='Miguel Boland, Maxim Scheremetjew',
    author_email='mdb@ebi.ac.uk, maxim@ebi.ac.uk',
    version=version,
    packages=['ena_portal_api', 'mgnify_backlog', 'mgnify_util'],
    install_requires=install_requirements,
    include_package_data=True,
    install_requirements=['emg-backlog-schema>=0.8.2'],
    dependency_links=[
        'https://github.com/EBI-Metagenomics/emg-backlog-schema/tarball/master#egg=emg-backlog-schema-0.8.2'
    ],
    entry_points={
        'console_scripts': [
            'create_request=analysis_request_cli.create_request:main',
            'complete_request=analysis_request_cli.complete_request:main',
            'edit_annotation_job=analysis_request_cli.edit_annotation_job:main'
        ],
    },
    tests_require=test_requirements,
    test_suite="tests",
    setup_requires=['pytest-runner'],
)
