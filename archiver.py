from flask import Flask, request
from flask_cors import CORS, cross_origin
import logging

import aux


# WSGI application name
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# Data archive storage location
datapath = aux.get_config('storage')

# Configure the log file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handle = logging.FileHandler('archiver.log')
log_handle.setLevel(logging.INFO)
log_format = logging.Formatter('%(asctime)s - %(message)s')
log_handle.setFormatter(log_format)
logger.addHandler(log_handle)


@app.errorhandler(404)
@cross_origin()
def not_found(error):
    """Return an error if route does not exist."""
    logger.info('404 Not found')
    return aux.responder('Not found', 404)


@app.route('/')
@cross_origin()
def index():
    """Base path, server listening check."""
    logger.info('Base path access')
    return aux.responder('PBDB data archive API running', 200)


@app.route('/schema')
@cross_origin()
def schema():
    """Base path, server listening check."""
    logger.info('Database schema access')
    return aux.schema_read()


@app.route('/archives/list')
@cross_origin()
def info():
    """Return information about existing data archives."""
    logger.info('List path access')
    return aux.archive_summary()


@app.route('/archives/retrieve/<int:archive_no>', methods=['GET'])
@cross_origin()
def retrieve(archive_no):
    """Retrieve an existing archive given an archive number."""
    from flask import send_from_directory

    filename = ''.join([str(archive_no), '.bz2'])

    file_type = aux.get_file_type(archive_no)

    attachment_filename = ''.join(['pbdb_archive_',
                                   str(archive_no),
                                   file_type,
                                   '.bz2'])

    if archive_no:
        try:
            return send_from_directory(datapath,
                                       filename,
                                       as_attachment=True,
                                       attachment_filename=attachment_filename,
                                       mimetype='application/x-compressed')

            logger.info('Retrieved archive {0:d}'.format(archive_no))

        except Exception as e:
            logger.info('Retrieval error archive {0:d}'.format(archive_no))
            logger.info(e)
            return aux.responder('Retrieval error', 500)

    else:
        logger.info('Unspecified archive number')
        return aux.responder('Unspecified archive number', 400)


@app.route('/archives/view/<int:archive_no>', methods=['GET'])
@cross_origin()
def view(archive_no):
    """Retrieve details on a single archive."""
    logger.info('View path access')
    return aux.view_archive(archive_no)


@app.route('/archives/delete/<int:archive_no>', methods=['GET'])
@cross_origin()
def delete(archive_no):
    """Delete a archive record from the system table."""
    try:
        aux.delete_archive(archive_no)
        return aux.responder('Success', 200)
    except Exception as e:
        logger.info('Deletion error: {0:s}'.format(e))
        return aux.responder('Deletion error', 500)


@app.route('/archives/update/<int:archive_no>', methods=['POST', 'GET'])
@cross_origin()
def update(archive_no):
    """Update the archive metadata."""
    title = request.json.get('title')
    desc = request.json.get('description')
    authors = request.json.get('authors')
    doi = request.json.get('doi')

    if title or desc or authors or doi:
        try:
            aux.update_record(archive_no, title, desc, authors, doi)

        except Exception as e:
            logger.info(e)
            return aux.responder('Server error - record update', 500)

        logger.info('Updated {0:d}'.format(archive_no))
        return aux.responder('Success', 200)

    else:
        logger.info('ERROR: Unsupported parameters for update')
        return aux.responder('Parameter error', 400)


@app.route('/archives/create', methods=['POST'])
@cross_origin()
def create():
    """Create an archive file on disk."""
    import subprocess
    import os
    # from urllib.parse import quote

    # Attempt to find session_id in the payload (testing only)
    if request.json.get('session_id'):
        session_id = request.json['session_id']
    # Otherwise pull it out of the browser cookie (normal functionalty)
    else:
        session_id = request.cookies.get('session_id')

    # Determine authorizer and enter numbers from the session_id
    try:
        auth, ent = aux.user_info(session_id)
    except Exception as e:
        logger.info(e)
        return aux.responder('Client error - Invalid session ID', 400)

    # Determine if the user has an ORCID
    has_orcid = aux.check_for_orcid(ent)
    logger.info(f'Enter ID {ent} has ORCID {has_orcid}')
    if not has_orcid:
        return aux.responder('Missing ORCID', 403)

    # Extract user entered metadata from payload
    authors = request.json.get('authors', 'Enter No. ' + str(ent))
    title = request.json.get('title')
    desc = request.json.get('description', 'No description')

    # Extract components of data service call from payload
    path = request.json.get('uri_path')
    args = request.json.get('uri_args')

    # Parameter checks
    if not title:
        return aux.responder('Missing title', 400)

    if not args:
        return aux.responder('Missing uri_args', 400)

    if path:
        if path[0] != '/':
            return aux.responder('uri_path not preceeded by "/"', 400)
    else:
        return aux.responder('Missing uri_path', 400)

    # Build data service URI
    base = aux.get_config('dataservice')
    uri = ''.join([base, path, '?', args])
    uri = uri.replace(' ', '%20')

    # Initiate new record in database
    try:
        aux.create_record(auth, ent, authors, title, desc, path, args)
        logger.info('Record created. Enterer No: {0:d}'.format(ent))
    except Exception as e:
        logger.info(e)
        return aux.responder('Server error - Record creation', 500)

    # Read archive_no back from the table and create filename
    try:
        archive_no = aux.get_archive_no(ent)
        logger.info('Record created. Archive No: {0:d}'.format(archive_no))
    except Exception as e:
        logger.info(e)
        aux.archive_status(archive_no, success=False)
        return aux.responder('Server error - Archive number not found', 500)

    # Append the data path and remove extra "/" if one was added in config
    realpath = '/'.join([datapath, str(archive_no)])
    realpath = realpath.replace('//', '/')

    # Use cURL to retrive the dataset
    token = '='.join(['session_id', request.cookies.get('session_id')])
    headerpath = realpath + '.header'
    syscall = subprocess.run(['curl', '-s', '--cookie', token,
                              '-o', realpath, '-D', headerpath, uri])

    # Check to see that there were no errors in the data service return
    if syscall.returncode != 0 or not os.path.exists(headerpath):
        logger.info('Archive download error')
        aux.archive_status(archive_no, success=False)
        return aux.responder('Server error - File retrieval', 500)
    with open(headerpath, 'r') as f:
        content = f.readlines()
        if '200' not in content[0]:
            logger.info('Data service error')
            aux.archive_status(archive_no, success=False)
            return aux.responder('Server error - Data service', 500)

    # Compress and replace the retrieved dataset on disk
    syscall = subprocess.run(['bzip2', '-f', realpath])
    if syscall.returncode != 0:
        logger.info('Archive compression error')
        aux.archive_status(archive_no=archive_no, success=False)
        return aux.responder('Server error - File compression', 500)

    # Archive was successfully created on disk
    logger.info('Created archive number: {0:d}'.format(archive_no))
    aux.archive_status(archive_no=archive_no, success=True)

    # Dispatch email requesting DOI
    result = aux.request_doi(archive_no, title)
    print(f'Email response code: {result}')
    if not result[0]:
        logger.info('Server error - Email failure')
    else:
        logger.info('DOI email sent')

    # Return 200 OK
    return aux.responder('Success', 200)
