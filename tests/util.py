from backlog.models import *
from tests.test_mgnify_backlog import mgnify, ena


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
    'accession': 'ERR12345_test',
    'last_updated': '2018-11-21'
}

user_data = {
    'webin_id': 'Webin-460',
    'email_address': 'test@test.com',
    'first_name': 'John',
    'surname': 'Doe'
}

assembly_data = {'accession': 'GCA_001751075', 'assembly_level': 'scaffold', 'assembly_name': 'ASM175107v1',
                 'assembly_title': 'ASM175107v1 assembly for Desulfobacterales bacterium S7086C20',
                 'base_count': '2653970', 'genome_representation': 'full', 'sample_accession': 'SAMN05301627',
                 'scientific_name': 'Desulfobacterales bacterium S7086C20', 'secondary_sample_accession': '',
                 'strain': '', 'study_accession': 'PRJNA326769',
                 'study_description': "Two samples ...",
                 'study_name': 'marine sediment metagenome',
                 'study_title': 'marine sediment metagenome Raw sequence reads', 'tax_id': '1869302',
                 'last_updated': '2016-10-03'
                 }


def create_annotation_jobs(rt_ticket=0, priority=0):
    study = mgnify.create_study_obj(study_data)
    accessions = ['ERR164407', 'ERR164408', 'ERR164409']
    lineage = 'root:Host-Associated:Human:Digestive System'

    runs = [mgnify.get_or_save_run(ena, accession, study=study, lineage=lineage) for accession in accessions]
    pipeline = Pipeline(version=4.1)
    pipeline.save()

    user = mgnify.create_user(user_data['webin_id'], user_data['email_address'], user_data['first_name'],
                              user_data['surname'])
    request = mgnify.create_user_request(user, priority, rt_ticket)

    assert len(AnnotationJob.objects.all()) == 0

    mgnify.create_annotation_job(request, runs[0], priority)
    mgnify.create_annotation_job(request, runs[1], priority)
    mgnify.create_annotation_job(request, runs[2], priority)
    return study, runs