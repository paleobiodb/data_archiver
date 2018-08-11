from flask import Flask, request, make_response, jsonify

# Name the WSGI application
app = Flask(__name__)

# Data archive storage location
DATA_PATH = '~/scratch/archive-test'

def responder(msg, status):
    """Format a JSON response."""
    return make_response(jsonify({'message': msg,
                                  'status': status}), status)


@app.errorhandler(404)
def not_found(error):
    """Return an error if route does not exist."""
    return make_response(jsonify({'message': 'Not found',
                                 'status': 404}, 404))


@app.route('/')
def index():
    """Root path, server listening check."""
    return responder('PBDB data archive system operational', 200)



@app.route('/info')
def info():
    """Return information about existing data archives."""
    return 'Info route'


@app.route('/retrieve', methods=['GET'])
def retrieve():
    """Retrieve an existing archive by DOI reference."""
    from flask import send_from_directory

    doi = request.args.get('doi', default='None', type=str)
    
    if doi:
        #XXX lookup filename by doi
        filename = doi

        return send_from_directory(DATA_PATH,
                                   filename,
                                   as_attachment=True,
                                   attachment_filename=filename,
                                   mimetype='application/x-compressed')


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
        realpath = '/'.join([DATA_PATH, filename])
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
