from setuptools import setup, find_packages

import os

version = "0.1.0"

_base = os.path.dirname(os.path.abspath(__file__))
_requirements = os.path.join(_base, 'requirements.txt')
# _requirements_test = os.path.join(_base, 'requirements-test.txt')
_env_activate = os.path.join(_base, 'venv', 'bin', 'activate')

install_requirements = []
with open(_requirements) as f:
    install_requirements = f.read().splitlines()

# test_requirements = []
# if "test" in sys.argv:
#     with open(_requirements_test) as f:
#         test_requirements = f.read().splitlines()


setup(
    name="emg_libs",
    version=version,
    packages=find_packages(exclude=['ez_setup']),
    install_requires=install_requirements,
    include_package_data=True,
    setup_requires=['pytest-runner'],
    dependency_links= [
        'https://github.com/EBI-Metagenomics/emg-backlog-schema/tarball/master'
    ]
    # tests_require=test_requirements,
    # test_suite="tests",
)
