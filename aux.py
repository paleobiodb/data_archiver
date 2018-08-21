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


def update_record(archive_no, title, desc):
    """Add metadata to the archive table in database."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')
    cursor = db.cursor()

    if title:
        sql = """UPDATE data_archives
                 SET title = '{0:s}'
                 WHERE archive_no = {1:d};
              """.format(title, archive_no)
        cursor.execute(sql)

    if desc:
        sql = """UPDATE data_archives
                 SET description = '{0:s}'
                 WHERE archive_no = {1:d};
              """.format(desc, archive_no)
        cursor.execute(sql)

    db.commit()
    db.close()
