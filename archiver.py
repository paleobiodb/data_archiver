from flask import Flask, request, make_response, jsonify

app = Flask(__name__)

def responder(msg, status):
    return make_response(jsonify({'message': msg,
                                  'status': status}), status)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'message': 'Not found',
                                 'status': 404}, 404))


@app.route('/')
def index():
    return 'PBDB data archive system'


@app.route('/info')
def info():
    """Return information about existing data archives."""
    return 'Info route'


@app.route('/retrieve')
def retrieve():
    """Retrieve an existing archive by DOI reference."""
    doi = request.args.get('doi', default='None', type=str)
    return 'Retrieving DOI {0:s}'.format(doi)


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
            return responder('Invalid URI', 400)
        elif uri[:22] != 'https://paleobiodb.org':
            return responder('Invalid URI', 400)
        elif len(uri.split()) > 1:
            return responder('Invalid URI', 400)
        elif '&&' in uri:
            return responder('Invalid URI', 400)

        # Create a ISO format UTC timestamp for the archive
        timestamp = '{0:s}Z'.format(dt.isoformat(dt.utcnow(),
                                                 timespec='minutes'))

        # Determine the correct file name extention for the dataset
        for extention in extentions.keys():
            if extention in uri:
                file_ext = extentions[extention]
        if not file_ext:
            return responder('Invalid URI', 400)

        # Create a filename from the first 8 characters of a hash of the URI
        # together with the appended file extention
        filename = '.'.join([md5(uri.encode()).hexdigest()[:8], file_ext])

        # Use cURL to retrive the dataset
        syscall = subprocess.run(['curl', '-s', uri, '-o', filename])
        if syscall.returncode != 0:
            return responder('Server error', 500)

        # Compress and replace the retrieved dataset on disk
        syscall = subprocess.run(['bzip2', '-f', filename])
        if syscall.returncode != 0:
            return responder('Server error', 500)

        # Archive was successfully created on disk
        return responder('Success', 200)

    else:
        return responder('No URI specified', 400)
