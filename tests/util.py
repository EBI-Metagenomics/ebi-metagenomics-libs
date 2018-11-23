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
    'last_updated': '2018-05-05'
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

assembly_data = {
    'primary_accession': 'ERR12345_test'
}

user_data = {
    'webin_id': 'Webin-460',
    'email_address': 'test@test.com',
    'first_name': 'John',
    'surname': 'Doe'
}
