from flask import make_response, jsonify

def responder(msg, status):
    """Format a JSON response."""
    return make_response(jsonify({'message': msg,
                                  'status': status}), status)


def get_config(setting):
    """Retrive archive storage path from settings file."""
    import configparser

    config = configparser.ConfigParser()
    config.read('settings.cnf')

    return str(config['environment'][setting])


def user_info(session_id):
    """Retrieve authorizer and enterer numbers based on browser cookie."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """SELECT authorizer_no, enterer_no
             from session_data
             where session_id = '{0:s}'
          """.format(session_id)

    cursor.execute(sql)

    return doi_map


def archive_names():
    """Return a hash of DOIs and actual filenames."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
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

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """SELECT title, doi, username, creation_date, description, uri
             FROM data_archives;
          """
    cursor.execute(sql)

    archives = list()
    for title, doi, username, creation_date, description, uri in cursor:
        archives.append({'title': title,
                         'doi': doi,
                         'username': username,
                         'creation_date': creation_date,
                         'description': description,
                         'uri': uri})

    db.close()

    return jsonify(archives)


def create_record(timestamp, uri, filename):
    """Create new record in database."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """INSERT INTO data_archives (creation_date, uri, filename)
             VALUES ('{0:s}', '{1:s}', '{2:s}');
          """.format(timestamp, uri, filename)
             
    try:
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()

    db.close()


def update_record(archive_no, title, desc, doi):
    """Add metadata to the archive table in database."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')
    cursor = db.cursor()

    if title:
        title = title[:100]
        sql = """UPDATE data_archives
                 SET title = '{0:s}'
                 WHERE archive_no = {1:d};
              """.format(title, archive_no)
        cursor.execute(sql)

    if desc:
        desc = desc[:5000]
        sql = """UPDATE data_archives
                 SET description = '{0:s}'
                 WHERE archive_no = {1:d};
              """.format(desc, archive_no)
        cursor.execute(sql)

    if doi:
        doi = doi[:100]
        sql = """UPDATE data_archives
                 SET doi = '{0:s}'
                 WHERE archive_no = {1:d};
              """.format(doi, archive_no)
        cursor.execute(sql)

    db.commit()
    db.close()
