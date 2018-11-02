# ebi-metagenomics-libs
This repository contains internal tools and libs used by the MGnify team at EMBL-EBI.

## Installation
```bash
pip install -U git+git://github.com/EBI-Metagenomics/ebi-metagenomics-libs.git@analysis-request-cli
```
## Setting up analysis_request_cli
The following environment vars must be defined:
 * ENA_API_USER: MGnify username for the ENA search Portal
 * ENA_API_PASSWORD: MGnify username for the ENA search Portal
 * MGNIFY_API_PASSWORD: password for MGnify API
 * BACKLOG_CONFIG: path to django config file