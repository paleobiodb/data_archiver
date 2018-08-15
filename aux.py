from flask import make_response, jsonify

def responder(msg, status):
    """Format a JSON response."""
    return make_response(jsonify({'message': msg,
                                  'status': status}), status)


def archive_location():
    """Retrive archive storage path from settings file."""
    import configparser

    config = configparser.ConfigParser()
    config.read('settings.cnf')

    return str(config['environment']['storage'])


def archive_names():
    """Return a hash of DOIs and actual filenames."""
    import MySQLdb

    db_handle = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db_handle.cursor()
    sql = """SELECT doi, filename
             FROM data_archives;
          """
    cursor.execute(sql)

    doi_map = dict()
    for doi, filename in cursor:
        doi_map[doi.lower()] = filename

    return doi_map


def archive_summary():
    """Load archive information from database."""
    import MySQLdb

    db_handle = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db_handle.cursor()
    sql = """SELECT username, filename, doi
             FROM data_archives;
          """
    cursor.execute(sql)

    archives = list()
    for username, filename, doi in cursor:
        archives.append({'username': username,
                         'filename': filename,
                         'doi': doi})

    return jsonify(archives)


def create_record(timestamp, filename, uri):
    """Create new record in database."""
    import MySQLdb

    db_handle = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db_handle.cursor()
    sql = """INSERT INTO data_archives (timestamp, filename, uri)
             VALUES ({0:s}, {1:s}, {2:s})
          """.format(timestamp, filename, uri)
             
    cursor.execute(sql)
