from flask import Flask, request
import aux
import logging
from flask_cors import CORS, cross_origin

# WSGI application name
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# Data archive storage location
datapath = aux.archive_location()

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
    return aux.responder('PBDB data archive system operational', 200)


@app.route('/list')
@cross_origin()
def info():
    """Return information about existing data archives."""
    logger.info('List path access')
    return aux.archive_summary()


@app.route('/retrieve', methods=['GET'])
@cross_origin()
def retrieve():
    """Retrieve an existing archive given a DOI."""
    from flask import send_from_directory

    # Retrieve DOI from the parameter list
    doi = request.args.get('doi', default='None', type=str).lower()
    
    if doi:
        # Load DOI:filename map from database
        doi_map = aux.archive_names()

        # Match the DOI to the archive filename on disk
        if doi in doi_map.keys():
            filename = doi_map[doi]
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


@app.route('/update/<int:archive_no>', methods=['PUT'])
@cross_origin()
def update(archive_no):
    """Update the archive metadata."""
    title = request.json.get('title')
    desc = request.json.get('desc')
    doi = request.json.get('doi')

    if title or desc or doi:
        try:
            aux.update_record(archive_no, title, desc, doi)
        except Exception as e:
            print(e)
            return aux.responder('Server error - record update', 500)

        return aux.responder('Success', 200)

    else:
        return aux.responder('Parameter error', 400)


@app.route('/create', methods=['PUT'])
@cross_origin()
def create():
    """Create an archive file on disk."""
    from datetime import datetime as dt
    from hashlib import md5
    import subprocess

    # Dictionary of available file name extentions
    extentions = ['.csv', '.json', '.tsv', '.ris']

    # Retrieve the URI from the submitted payload
    uri = request.json.get('uri').replace(' ', '%20')

    if uri:
        # Check the validity of the passed API call
        if type(uri) != str:
            return aux.responder('Invalid URI', 400)
        elif uri[:22] != 'https://paleobiodb.org':
            return aux.responder('Invalid URI', 400)
        elif len(uri.split()) > 1:
            return aux.responder('Invalid URI', 400)
        elif '&&' in uri:
            return aux.responder('Invalid URI', 400)

        # Create a ISO format UTC timestamp for the archive
        timestamp = '{0:s}Z'.format(dt.isoformat(dt.utcnow(),
                                                 timespec='minutes'))

        # Determine the correct file name extention for the dataset
        file_ext = None
        for extention in extentions:
            if extention in uri:
                file_ext = extention
        if not file_ext:
            return aux.responder('Invalid URI', 400)

        # Create a filename from the first 8 characters of a hash of the URI
        # together with the appended file extention
        filename = ''.join([md5(uri.encode()).hexdigest()[:8], file_ext])

        # Append the data path and remove extra "/" if one was added in config
        realpath = '/'.join([datapath, filename])
        realpath = realpath.replace('//', '/')

        # Use cURL to retrive the dataset
        syscall = subprocess.run(['curl', '-s', uri, '-o', realpath])
        if syscall.returncode != 0:
            return aux.responder('Server error - file retrieval', 500)

        # Compress and replace the retrieved dataset on disk
        syscall = subprocess.run(['bzip2', '-f', realpath])
        if syscall.returncode != 0:
            return aux.responder('Server error - file compression', 500)

        # Initiate new record in database
        try:
            aux.create_record(timestamp, uri, filename)
        except Exception as e:
            print(e)
            return aux.responder('Server error - record creation', 500)

        # Archive was successfully created on disk
        return aux.responder('Success', 200)

    else:
        return aux.responder('No URI specified', 400)
