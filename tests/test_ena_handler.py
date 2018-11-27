import pytest
import os

from ena_portal_api import ena_handler


class MockResponse:
    def __init__(self, status_code, data=None, text=None):
        self.data = data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.data


class TestEnaHandler(object):
    def test_authentication_set(self):
        auth = ('username', 'password')
        os.environ['ENA_API_USER'], os.environ['ENA_API_PASSWORD'] = auth
        ena = ena_handler.EnaApiHandler()
        assert ena.auth == auth

    def test_authentication_not_set(self):
        if os.environ['ENA_API_USER']:
            del os.environ['ENA_API_USER']
        if os.environ['ENA_API_PASSWORD']:
            del os.environ['ENA_API_PASSWORD']
        ena = ena_handler.EnaApiHandler()
        assert ena.auth is None

    def test_get_study_primary_accession_should_retrieve_study_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        study = ena.get_study('ERP001736')
        assert isinstance(study, dict)
        assert len(study.keys()) == 10

    @pytest.mark.parametrize('accession', ('ERP001736', 'PRJEB1787'))
    def test_get_study_secondary_accession_should_retrieve_study_all_fields(self, accession):
        ena = ena_handler.EnaApiHandler()
        study = ena.get_study(accession)
        assert isinstance(study, dict)
        assert len(study.keys()) == 10

    @pytest.mark.parametrize('accession', ('ERP001736', 'PRJEB1787'))
    def test_get_study_secondary_accession_should_retrieve_study_filtered_fields(self, accession):
        ena = ena_handler.EnaApiHandler()
        study = ena.get_study(accession, fields='study_accession')
        assert isinstance(study, dict)
        assert len(study.keys()) == 1
        assert 'study_accession' in study

    def test_get_study_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_study('Invalid accession')

    def test_get_study_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_study('ERP001736')

    def test_get_study_api_no_results(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(204, text=None)
        with pytest.raises(ValueError):
            ena.get_study('ERP001736')

    def test_get_run_should_retrieve_run_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        run = ena.get_run('ERR1701760')
        assert isinstance(run, dict)
        assert len(run) == 14

    def test_get_run_should_retrieve_run_filtered_fields(self):
        ena = ena_handler.EnaApiHandler()
        run = ena.get_run('ERR1701760', fields='run_accession')
        assert isinstance(run, dict)
        assert len(run) == 1
        assert 'run_accession' in run

    def test_get_run_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_run('Invalid accession')

    def test_get_run_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_run('ERR1701760')

    def test_get_study_runs_should_have_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        runs = ena.get_study_runs('SRP125161')
        assert len(runs) == 4
        for run in runs:
            assert len(run) == 14
            assert isinstance(run, dict)

    def test_get_study_runs_should_have_filter_run_accessions(self):
        ena = ena_handler.EnaApiHandler()
        runs = ena.get_study_runs('SRP125161', filter_accessions=['SRR6301444'])
        assert len(runs) == 1
        for run in runs:
            assert len(run) == 14
            assert isinstance(run, dict)

    def test_get_study_runs_should_not_fetch_size_if_private(self):
        ena = ena_handler.EnaApiHandler()
        runs = ena.get_study_runs('SRP125161', filter_accessions=['SRR6301444'], private=True)
        assert len(runs) == 1
        for run in runs:
            assert len(run) == 14
            assert isinstance(run, dict)
            assert run['raw_data_size'] is None

    def test_get_study_runs_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_study_runs('Invalid accession')

    def test_get_study_runs_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_study_runs('SRP125161')

    def test_get_study_assemblies_should_have_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        assemblies = ena.get_study_assemblies('PRJNA326769')
        for assembly in assemblies:
            assert len(assembly) == 16
            assert isinstance(assembly, dict)

    def test_get_study_assemblies_should_filter_fields(self):
        ena = ena_handler.EnaApiHandler()
        assemblies = ena.get_study_assemblies('PRJNA326769', fields='accession,study_accession')
        for assembly in assemblies:
            assert len(assembly) == 2
            assert isinstance(assembly, dict)

    def test_get_study_assemblies_should_filter_accessions(self):
        ena = ena_handler.EnaApiHandler()
        assemblies = ena.get_study_assemblies('PRJNA326769', filter_accessions=['GCA_001751075'])
        for assembly in assemblies:
            assert len(assembly) == 16
            assert isinstance(assembly, dict)

    def test_get_study_assemblies_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_study_assemblies('Invalid accession')

    def test_get_study_assemblies_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_study_assemblies('PRJNA326769')

    def test_get_assembly_should_have_all_fields(self):
        ena = ena_handler.EnaApiHandler()
        assembly = ena.get_assembly('GCA_001751075')
        assert len(assembly) == 16
        assert isinstance(assembly, dict)

    def test_get_assembly_should_filter_fields(self):
        ena = ena_handler.EnaApiHandler()
        assembly = ena.get_assembly('GCA_001751075', fields='accession,study_accession')
        assert len(assembly) == 2
        assert isinstance(assembly, dict)

    def test_get_assembly_api_unavailable(self):
        ena = ena_handler.EnaApiHandler()
        ena.post_request = lambda r: MockResponse(500)
        with pytest.raises(ValueError):
            ena.get_assembly('GCA_001751075')

    def test_get_assembly_invalid_accession(self):
        ena = ena_handler.EnaApiHandler()
        with pytest.raises(ValueError):
            ena.get_assembly('Invalid_accession')

    def test_download_runs(self, tmpdir):
        tmpdir = tmpdir.strpath
        current_dir = os.getcwd()
        os.chdir(tmpdir)
        run = {'fastq_ftp': 'ftp.sra.ebi.ac.uk/vol1/fastq/ERR866/ERR866589/ERR866589_1.fastq.gz;'
                            'ftp.sra.ebi.ac.uk/vol1/fastq/ERR866/ERR866589/ERR866589_2.fastq.gz'}
        ena_handler.download_runs([run])
        fs = os.listdir(tmpdir)
        assert len(fs) == 2
        assert 'ERR866589_1.fastq.gz' in fs
        assert 'ERR866589_2.fastq.gz' in fs

        os.chdir(current_dir)

    def test_get_study_run_accessions_should_return_all_accessions(self):
        ena = ena_handler.EnaApiHandler()
        assert set(ena.get_study_run_accessions('ERP000339')) == {'ERR019477', 'ERR019478'}

    def test_get_study_run_accessions_should_not_return_amplicons(self):
        ena = ena_handler.EnaApiHandler()
        assert len(ena.get_study_run_accessions('SRP118880')) == 10

    def test_get_study_run_accessions_should_return_all_accessions_including_amplicon(self):
        ena = ena_handler.EnaApiHandler()
        assert len(ena.get_study_run_accessions('SRP118880', filter_assembly_runs=False)) == 390

    def test_get_study_assembly_accessions_should_return_all_accessions(self):
        ena = ena_handler.EnaApiHandler()
        assert set(ena.get_study_assembly_accessions('PRJNA326769')) == {'GCA_001751075', 'GCA_001751165'}
