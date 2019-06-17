from flask import make_response, jsonify


def responder(msg, status, pbdb_id=None):
    """Format a JSON response."""
    if pbdb_id:
        return make_response(jsonify({'message': msg,
                                      'status': status,
                                      'pbdb_id': pbdb_id}), status)
    else:
        return make_response(jsonify({'message': msg,
                                      'status': status}), status)


def get_config(setting):
    """Retrive archive storage path from settings file."""
    import configparser

    config = configparser.ConfigParser()
    config.read('settings.cnf')

    return str(config['environment'][setting])


def request_doi(archive_no, title, yr, authors, ent):
    """Dispatch email to specified addresses using UNIX sendmail."""
    from email.mime.text import MIMEText
    from subprocess import Popen, PIPE

    try:
        ent_email = get_ent_email(ent)
    
        base = get_config('base')
        default_emails = get_config('email')
        email_addr = ','.join([default_emails, ent_email])
    
        body = f'URL: {base}/classic/app/archive/view?id={archive_no}\n'
        body += f'Creators: {authors}\n'
        body += f'Title: {title}\n'
        body += 'Publisher: Paleobiology Database\n'
        body += f'Publication Year: {yr}\n'
        body += 'Resource Type: Dataset\n'
        body += '========\n'
        body += f'PBDB Archive ID Number: {archive_no}\n'
        body += 'DOI: Pending\n'
    
        msg = MIMEText(body)
    
        msg['From'] = 'do-not-reply@paleobiodb.org'
        msg['To'] = email_addr
        msg['Subject'] = 'PBDB archive DOI request'
    
        p = Popen(['/usr/sbin/sendmail', '-t', '-oi'], stdin=PIPE)
    
        return p.communicate(msg.as_bytes())

    except exception as e:
        return e


def check_for_orcid(ent):
    """Check to see if a user has a stored ORCID."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """SELECT orcid
             FROM pbdb_wing.users
             WHERE person_no = {0:d}
          """.format(ent)

    cursor.execute(sql)

    for orcid in cursor:
        orcid = orcid[0]

    return False if orcid == '' else True


def get_ent_email(ent):
    """Retrieve user email from the database."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """SELECT email
             FROM pbdb_wing.users
             WHERE person_no = {0:d}
          """.format(ent)

    cursor.execute(sql)

    for email in cursor:
        ent_email = email[0]

    return ent_email


def admin_check(session_id):
    """Validate credentials for update and create."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """SELECT user_id
             FROM session_data
             WHERE session_id = '{0:s}'
          """.format(session_id)

    cursor.execute(sql)

    for user_id in cursor:
        user_id = user_id[0]

    cursor = db.cursor()
    sql = """SELECT admin
             FROM pbdb_wing.users
             WHERE id = '{0:s}'
          """.format(user_id)

    cursor.execute(sql)

    for admin in cursor:
        admin = admin[0]

    return False if admin == 0 else True


def user_info(session_id):
    """Retrieve authorizer and enterer numbers based on browser cookie."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """SELECT authorizer_no, enterer_no
             FROM session_data
             WHERE session_id = '{0:s}'
          """.format(session_id)

    cursor.execute(sql)

    for authorizer_no, enterer_no in cursor:
        auth = authorizer_no
        ent = enterer_no

    return auth, ent


def view_archive(archive_no):
    """Retrieve metadata for a single record."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    sql = """SELECT archive_no, title, doi, authors, created,
                    description, uri_path, uri_args
             FROM data_archives
             WHERE archive_no = {0:d}
             LIMIT 1
          """.format(archive_no)

    cursor.execute(sql)

    archives = list()
    for archive_no, title, doi, authors, created, description, \
            uri_path, uri_base in cursor:

            archives.append({'archive_no': archive_no,
                             'title': title,
                             'doi': doi,
                             'authors': authors,
                             'created': created,
                             'description': description,
                             'uri_path': uri_path,
                             'uri_base': uri_base})

    db.close()

    return jsonify(archives)


def delete_archive(archive_no):
    """Permanently remove a dataset from the system."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    sql = """DELETE FROM data_archives
             WHERE archive_no = {0:d}
             LIMIT 1
          """.format(archive_no)

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()

    db.close()

    # TODO: delete from file system?


def archive_names():
    """Return a hash of DOIs and actual filenames."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    sql = """SELECT doi, filename
             FROM data_archives
          """

    cursor.execute(sql)

    doi_map = dict()
    for doi, filename in cursor:
        doi_map[doi.lower()] = filename

    return doi_map


def schema_read():
    """Dump the header info to check db connector."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    sql = """SHOW COLUMNS
             FROM data_archives
          """

    cursor.execute(sql)

    schema = list()
    for row in cursor:
        schema.append(row)

    return make_response(jsonify(schema))


def archive_summary():
    """Load archive information from database."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    sql = """SELECT archive_no, title, doi, authors, created,
                    description, uri_path, uri_args
             FROM data_archives
          """

    cursor.execute(sql)

    archives = list()
    for archive_no, title, doi, authors, created, description, \
            uri_path, uri_base in cursor:

            archives.append({'archive_no': archive_no,
                             'title': title,
                             'doi': doi,
                             'authors': authors,
                             'created': created,
                             'description': description,
                             'uri_path': uri_path,
                             'uri_base': uri_base})

    db.close()

    return jsonify(archives)


def archive_status(archive_no, success):
    """Set the archive creation status in the table."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    if success:
        sql = """UPDATE data_archives
                 SET status = '{0:s}'
                 WHERE archive_no = {1:d}
              """.format('complete', archive_no)
    else:
        sql = """UPDATE data_archives
                 SET status = '{0:s}'
                 WHERE archive_no = {1:d}
              """.format('fail', archive_no)

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()

    db.close()


def get_archive_no(ent):
    """Determine the last incremented number generated by the active user."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    sql = """SELECT archive_no
             FROM data_archives
             WHERE enterer_no = {0:d}
             ORDER BY created DESC
             LIMIT 1
          """.format(ent)

    cursor.execute(sql)

    for archive_no in cursor:
        current_archive = archive_no[0]

    db.close()

    return current_archive


def get_file_type(archive_no):
    """Determine file type of the archive and return an extension."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()

    sql = """SELECT uri_path
             FROM data_archives
             WHERE archive_no = {0:d}
             LIMIT 1
          """.format(archive_no)

    cursor.execute(sql)

    for uri_path in cursor:
        uri_path = uri_path[0]

    db.close()

    return uri_path[uri_path.rfind('.'):]


def create_record(auth, ent, authors, title, desc, path, args):
    """Create new record in database."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')

    cursor = db.cursor()
    sql = """INSERT INTO data_archives
             (authorizer_no, enterer_no, authors, title, description,
              uri_path, uri_args)
             VALUES ({0:d}, {1:d}, '{2:s}', '{3:s}', '{4:s}', '{5:s}', '{6:s}')
          """.format(auth, ent, authors, title, desc, path, args)

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()
        raise ValueError(e)

    db.close()


def update_record(archive_no, title, desc, authors, doi):
    """Add metadata to the archive table in database."""
    import MySQLdb

    db = MySQLdb.connect(read_default_file='./settings.cnf')
    cursor = db.cursor()

    if title:
        title = title[:255]
        sql = """UPDATE data_archives
                 SET title = '{0:s}', modified = now()
                 WHERE archive_no = {1:d}
              """.format(title, archive_no)
        cursor.execute(sql)

    if desc:
        desc = desc[:5000]
        sql = """UPDATE data_archives
                 SET description = '{0:s}', modified = now()
                 WHERE archive_no = {1:d}
              """.format(desc, archive_no)
        cursor.execute(sql)

    if authors:
        desc = desc[:255]
        sql = """UPDATE data_archives
                 SET authors = '{0:s}', modified = now()
                 WHERE archive_no = {1:d}
              """.format(desc, archive_no)
        cursor.execute(sql)

    if doi:
        doi = doi[:100]
        sql = """UPDATE data_archives
                 SET doi = '{0:s}', modified = now()
                 WHERE archive_no = {1:d}
              """.format(doi, archive_no)
        cursor.execute(sql)

    try:
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()

    db.close()
