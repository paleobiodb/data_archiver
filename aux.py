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

    return config['environment']['storage']


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
