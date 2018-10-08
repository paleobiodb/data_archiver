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


@app.route('/archives/retrieve/<int:archive_no>', methods=['PUT'])
@cross_origin()
def retrieve(archive_no):
    """Retrieve an existing archive given an archive number."""
    from flask import send_from_directory

    filename = ''.join([str(archive_no), '.bz2'])
    attachment_filename = 'pbdb_archive_' + str(archive_no) + '.bz2'

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
        logger.info('Unspecified archive number', 400)

    '''Retrieve give a DOI - Currently disabled

    # Retrieve DOI from the parameter list
    doi = request.args.get('doi', default='None', type=str).lower()

    if doi:
        # Load DOI:filename map from database
        doi_map = aux.archive_names()

        # Match the DOI to the archive filename on disk
        if doi in doi_map.keys():
            filename = doi_map[doi]
            logger.info('Retrieve {0:s} - {1:s}'.format(doi, fileneme))
            return send_from_directory(datapath,
                                       filename,
                                       as_attachment=True,
                                       attachment_filename=filename,
                                       mimetype='application/x-compressed')

        else:
            logger.info('ERROR: Invalid DOI specified')
            return aux.responder('Client error', 400)

    else:
        logger.info('ERROR: Unspecified DOI')
        return aux.responder('Client error', 400)
    '''


@app.route('/archives/delete/<int:archive_no>', methods=['PUT'])
@cross_origin()
def delete(archive_no):

    try:
        aux.delete_archive(archive_no)
    except Exception as e:
        logger.info('Deletion error archive number: {0:d}'.format(archive_no))


@app.route('/archives/update/<int:archive_no>', methods=['PUT'])
@cross_origin()
def update(archive_no):
    """Update the archive metadata."""
    title = request.json.get('title')
    desc = request.json.get('description')
    authors = request.json.get('authors')
    doi = request.json.get('doi')

    if title or desc or authors or doi:
        try:
            aux.update_record(archive_no, title, desc, doi)
        except Exception as e:
            logger.info(e)
            return aux.responder('Server error - record update', 500)

        return aux.responder('Success', 200)

    else:
        logger.info('ERROR: Unsupported parameters for update')
        return aux.responder('Parameter error', 400)


@app.route('/archives/create', methods=['PUT'])
@cross_origin()
def create():
    """Create an archive file on disk."""
    import subprocess
    from urllib.parse import quote
    
    #
    # Sample JSON PUT format for payload for create path
    #
    # frontent to quote all urls
    #
    # {'title': '', 'authors': '', 'description': '', 'uri_path': '',
    # 'uri_args': '', 'format': ''}
    #
    # uri_path eg. '/data1.2/occs/list.txt'
    # uri_args eg. 'base_name=canis&interval=miocene'
    #
    # Notes:
    #   1. Frontend to http encode (aka quote) the uri components
    #   2. Frontend will lead path with a '/'
    #   3. Backend to insert a "?" between path and args
    #   4. For testing from the command line with curl, you can pass
    #      a session_id key:val (grabbed from a current browser session)
    #      in the payload.
    #

    # Attempt to find session_id in the payload (testing only)
    if request.json.get('session_id'):
        session_id = request.json['session_id']
    # Otherwise pull it out of the browser cookie (normal functionalty)
    else:
        session_id = request.cookie.get('session_id')
    
    # Determine authorizer and enter numbers from the session_id
    try:
        # auth, ent = aux.user_info(request.cookie.get('session_id'))
        auth, ent = aux.user_info(session_id)
    except Exception as e:
        logger.info(e)
        return aux.responder('Client error - Invalid session ID', 400)

    # Extract user entered metadata from payload
    # TODO: alternative to authors should resove to username from wing table
    authors = request.json.get('authors', 'Enter No. ' + str(ent))
    title = request.json.get('title')
    desc = request.json.get('description', 'No description')

    # Extract components of data service call from payload
    path = request.json.get('uri_path')
    args = quote(request.json.get('uri_args'))

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

    '''
    # XXX debug
    print(auth)
    print(ent)
    print(uri)
    print(authors)
    print(title)
    print(desc)
    realpath = '/'.join([datapath, 'testfile'])
    realpath = realpath.replace('//', '/')
    syscall = subprocess.run(['touch', realpath])
    return aux.responder('pass', 200)
    '''

    # Initiate new record in database
    try:
        aux.create_record(auth, ent, authors, title, desc, path, args)
    except Exception as e:
        logger.info(e)
        return aux.responder('Server error - Record creation', 500)

    # Read archive_no back from the table and create filename
    try:
        archive_no = aux.get_archive_no(ent)
    except Exception as e:
        logger.info(e)
        aux.archive_status(archive_no, success=False)
        return aux.responder('Server error - Archive number not found', 500)

    # Append the data path and remove extra "/" if one was added in config
    realpath = '/'.join([datapath, str(archive_no)])
    realpath = realpath.replace('//', '/')

    # Use cURL to retrive the dataset
    print(realpath)
    syscall = subprocess.run(['curl', '-s', uri, '-o', realpath])
    if syscall.returncode != 0:
        logger.info('Archive download error')
        aux.archive_status(archive_no, success=False)
        return aux.responder('Server error - File retrieval', 500)

    # Compress and replace the retrieved dataset on disk
    syscall = subprocess.run(['bzip2', '-f', realpath])
    if syscall.returncode != 0:
        logger.info('Archive compression error')
        aux.archive_status(archive_no, success=False)
        return aux.responder('Server error - File compression', 500)

    # Archive was successfully created on disk
    logger.info('Created archive number: {0:d}'.format(archive_no))
    aux.archive_status(archive_no, success=True)
    return aux.responder('Success', 200)
