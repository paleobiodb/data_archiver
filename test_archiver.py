"""Test the PBDB Data Archive API by importing the original 20 archives."""

import time
import csv
import sys
import json
import pprint

import pytest
import requests


SERVER = 'https://paleobiodb.org'
SESSION = '634F5462-6091-11E9-BA9F-BA2CB069FCF4'
INFILE = 'archive_drivers.csv'
#  INFILE = 'one_rec.csv'

@pytest.fixture()
def load_metadata():
    """Load metadata for the archives from disk."""

    archives = []
    n_archives = 0

    with open(INFILE, 'r') as f:

        reader = csv.reader(f)
        for i, row in enumerate(reader):

            print()
            if row[0][0] == '#':
                print(f'Skipping archive {i+1}')
                continue

            for bad_text in ['opinions', 'taxa', 'occs']:
                if row[1].find(bad_text) != -1:
                    print(f'Bad data on row {i}')
                    print(row)
                    raise ValueError(f'Bad data on row {i}')

            if len(row) != 3:
                print(f'Missing data row {i}')
                print(row)
                raise ValueError(f'Missing data on row {i}')

            archive = {'title': row[0],
                       'description': 'testing',
                       'authors': row[1]}

            uri = row[2]
            archive.update(uri_path=f'/{uri[uri.find("org/")+4:uri.find("?")]}')
            archive.update(uri_args=uri[uri.find('?')+1:])

            archive.update(session_id=SESSION)

            archives.append(archive)

        return archives


def test_create_archives(load_metadata):
    """Create, view, list and download each archive."""

    archives = load_metadata

    for archive in archives:

        pprint.pprint(archive)

        # Create archive
        rc = requests.post(f'{SERVER}/archives/create', json=archive)
        assert rc.status_code == 200
        resp_c = rc.json()
        assert rc.status_code == 200
        assert resp_c.get('message') == 'success'
        assert 'pbdb_id' in resp_c
        pbdb_id = resp_c.get('pbdb_id')
        print(f'Created Archive ID {pbdb_id}')
        print('========')
        assert pbdb_id is not None
        assert pbdb_id > 0

        # View archive
        rv = requests.get(f'{SERVER}/archives/view/{pbdb_id}')
        assert rv.status_code == 200
        resp_v = rv.json()[0]
        assert 'title' in resp_v
        assert 'doi' in resp_v
        assert 'authors' in resp_v
        assert 'created' in resp_v
        assert 'description' in resp_v
        assert 'uri_path' in resp_v
        assert 'uri_base' in resp_v
        assert 'archive_no' in resp_v
        assert resp_v.get('archive_no') == pbdb_id

        # List archives
        rl = requests.get(f'{SERVER}/archives/list')
        assert rl.status_code == 200
        archive_list = rl.json()
        assert archive_list is not []
        assert len(archive_list) > 0
        current_found = False
        for a in archive_list:
            if a['archive_no'] == pbdb_id:
                current_found = True
        assert current_found

        # Retrieve archive
        rr = requests.get(f'{SERVER}/archives/retrieve/{pbdb_id}')
        assert rr.status_code == 200
        assert 'Content-Length' in rr.headers
        assert int(rr.headers['Content-Length']) > 512
        assert 'Content-Disposition' in rr.headers
        assert f'pbdb_archive_{pbdb_id}' in rr.headers['Content-Disposition']
