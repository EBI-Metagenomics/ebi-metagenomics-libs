from backlog.models import *


def clean_db():
    AssemblyJob.objects.all().delete()
    AssemblyJobStatus.objects.all().delete()

    AnnotationJob.objects.all().delete()

    RunAssembly.objects.all().delete()
    RunAssemblyJob.objects.all().delete()
    Run.objects.all().delete()
    Assembly.objects.all().delete()
    Study.objects.all().delete()

    UserRequest.objects.all().delete()
    User.objects.all().delete()

    Pipeline.objects.all().delete()
    Assembler.objects.all().delete()


study_data = {
    'study_accession': 'PRJEB1787',
    'secondary_study_accession': 'ERP001736',
    'study_title': 'Shotgun Sequencing of Tara Oceans DNA samples corresponding to size fractions for  prokaryotes.',
    'first_public': '2018-05-05',
    'last_updated': '2018-05-05',
    'scientific_name': 'human gut metagenome',
    'description': 'Study description'
}

run_data = {
    'run_accession': 'ERR164407',
    'base_count': 160808514,
    'read_count': 282806,
    'instrument_platform': 'LS454',
    'instrument_model': '454 GS FLX Titanium',
    'library_strategy': 'WGS',
    'library_layout': 'SINGLE',
    'library_source': 'METAGENOMIC',
    'last_updated': '2018-11-21',
    'lineage': 'root:Environmental:Aquatic:Marine',
    'raw_data_size': 12345
}

user_data = {
    'webin_id': 'Webin-460',
    'email_address': 'test@test.com',
    'first_name': 'John',
    'surname': 'Doe'
}

assembly_data = {
    "analysis_accession": "ERZ795049",
    "study_accession": "PRJEB30178",
    "secondary_study_accession": "ERP112609",
    "sample_accession": "SAMN05720147",
    "secondary_sample_accession": "SRS1687472",
    "analysis_title": "activated sludge metagenome 1",
    "analysis_type": "SEQUENCE_ASSEMBLY",
    "center_name": "EMG",
    "first_public": "2019-01-07",
    "last_updated": "2018-12-07",
    "study_title": "EMG produced TPA metagenomics assembly of the Active sludge microbial communities of municipal "
                   "wastewater-treating anaerobic digesters from Japan - AD_JPNAS3_MetaG metagenome "
                   "(activated sludge metagenome) data set.",
    "tax_id": "942017",
    "scientific_name": "activated sludge metagenome",
    "analysis_alias": "ERR164407",
    "study_alias": "activated sludge metagenome metagenomic assembly (assembled from PRJNA340507, "
                   "internal_id: d538b336-5fe0-46ef-a720-48017eda4913)",
    "submitted_bytes": "214299326;54",
    "submitted_md5": "6e3180ec655a47a0cc2c4535f8008caa;c3a387584a3383505c219720ffbf857b",
    "sample_alias": "Gp0138835",
    "broker_name": "EBI-EMG",
    "sample_title": "Active sludge microbial communities of municipal wastewater-treating anaerobic "
                    "digesters from Japan - AD_JPNAS3_MetaG",
    "status_id": "4",
    "sample_description": "Active sludge microbial communities of municipal wastewater-treating "
                          "anaerobic digesters from Japan - AD_JPNAS3_MetaG",
    "pipeline_name": "",
    "pipeline_version": "",
    "assembly_type": "primary metagenome",
    "description": "activated sludge metagenome 1",

}
