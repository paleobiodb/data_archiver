from flask import Flask, request
import aux

# WSGI application name
app = Flask(__name__)

# Data archive storage location
datapath = aux.archive_location()

@app.errorhandler(404)
def not_found(error):
    """Return an error if route does not exist."""
    return aux.responder('Not found', 404)


@app.route('/')
def index():
    """Base path, server listening check."""
    return aux.responder('PBDB data archive system operational', 200)


@app.route('/list')
def info():
    """Return information about existing data archives."""
    return aux.archive_summary()


@app.route('/retrieve', methods=['GET'])
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
            return aux.responder('Invalid DOI', 400)

    else:
        return aux.responder('Unspecified DOI', 400)


    #  return aux.responder('Invalid or unspecified doi', 400)


@app.route('/create', methods=['PUT'])
def create():
    """Create an archive file on disk."""
    from datetime import datetime as dt
    from hashlib import md5
    import subprocess

    # Dictionary of available file name extentions
    extentions = {'list.csv': 'csv',
                  'list.json': 'json',
                  'single.csv': 'csv',
                  'single.json': 'json'}

    # Retrieve the URI from the submitted payload
    uri = request.json.get('uri')

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
        for extention in extentions.keys():
            if extention in uri:
                file_ext = extentions[extention]
        if not file_ext:
            return aux.responder('Invalid URI', 400)

        # Create a filename from the first 8 characters of a hash of the URI
        # together with the appended file extention
        filename = '.'.join([md5(uri.encode()).hexdigest()[:8], file_ext])

        # Use cURL to retrive the dataset
        realpath = '/'.join([datapath, filename])
        syscall = subprocess.run(['curl', '-s', uri, '-o', filename])
        if syscall.returncode != 0:
            return aux.responder('Server error', 500)

        # Compress and replace the retrieved dataset on disk
        syscall = subprocess.run(['bzip2', '-f', filename])
        if syscall.returncode != 0:
            return aux.responder('Server error', 500)

        # Archive was successfully created on disk
        return aux.responder('Success', 200)

    else:
        return aux.responder('No URI specified', 400)
