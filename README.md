[![Build Status](https://travis-ci.org/EBI-Metagenomics/ebi-metagenomics-libs.svg?branch=master)](https://travis-ci.org/EBI-Metagenomics/ebi-metagenomics-libs)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b2c48cc9e87c48e8aa28ed9062387643)](https://www.codacy.com/app/mb1069/ebi-metagenomics-libs?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=EBI-Metagenomics/ebi-metagenomics-libs&amp;utm_campaign=Badge_Grade)

:warning: **THIS PROJECT HAS BEEN ARCHIVED** :warning: 

The funcionality has been merged into https://github.com/EBI-Metagenomics/mi-automation

# ebi-metagenomics-libs
This repository contains internal tools and libs used by the MGnify team at EMBL-EBI.

## Installation
```bash
pip install -U git+git://github.com/EBI-Metagenomics/ena-api-handler.git
pip install -U git+git://github.com/EBI-Metagenomics/emg-backlog-schema.git
pip install -U git+git://github.com/EBI-Metagenomics/ebi-metagenomics-libs.git
```
## Setting up analysis_request_cli
The following environment vars must be defined:
 * ENA_API_USER: MGnify username for the ENA search Portal
 * ENA_API_PASSWORD: MGnify username for the ENA search Portal
 * MGNIFY_API_PASSWORD: password for MGnify API
 * BACKLOG_CONFIG: path to django config file
