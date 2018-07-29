"""dataOps.py deals with operations pertaining to sqlite3 database for stack overflow
   scrapper in 'stackOverflowJobsScrapper.py'.
   Running 'dataOps' as main calls 'create_job_posting_relations', initialising the db.

   db relations:

   job_posting -
   PK(job_posting_id) | URL| author | title |description |pub_date |location

   category -
   PK(category_id) | category_name

   job_posting_category -
   PK/FK(job_posting_id) | PK/FK(category_id)
"""
import sqlite3 as sql

def get_db_connect(database_name_text):
    """Return sqlite3 connection to db specified by param. 'database_name_text'
    """
    try:
        db = sql.connect(database_name_text)
        return db
    except Exception:
        raise

def create_job_posting_relations(new_database_name_text):
    """Create relations for a database storing all job posting data. Db structure
       at top of file.
    """
    db_conn = get_db_connect(new_database_name_text)
    db_cursor = db_conn.cursor()

    try:
        # job posting relation
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS job_posting (
                                        job_posting_id INTEGER PRIMARY KEY,
                                        URL TEXT,
                                        author TEXT,
                                        title TEXT,
                                        description TEXT,
                                        pub_date TEXT,
                                        location TEXT
                                        )''')

        # category relation
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS category (
                                    category_id INTEGER PRIMARY KEY,
                                    category_name TEXT
                                    )''')

        # association table between relations 'category' & 'job_posting'
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS job_posting_category (
                                    job_posting_id INTEGER,
                                    category_id INTEGER,
                                    FOREIGN KEY (job_posting_id)
                                    REFERENCES job_posting(job_posting_id),
                                    FOREIGN KEY (category_id)
                                    REFERENCES category(category_id),
                                    PRIMARY KEY (job_posting_id, category_id)
                                    )''')
    except Exception:
        db_conn.rollback()
        raise

    finally:
        db_conn.commit()
        db_conn.close()

def insert_job_posting(jobPostXmlEle, db_conn):
    """Given a job posting in a 'xml.etree.ElementTree.Element', insert relevant
       data into database specified by param. 'db_conn'.
    """
    db_cursor = db_conn.cursor()

    try:
        # insert row into relation 'job_posting'
        db_cursor.execute('''INSERT INTO job_posting VALUES (?, ?, ?, ?, ?, ?, ?)''',
                          (None,
                           jobPostXmlEle.find('link').text,
                           jobPostXmlEle.find('{http://www.w3.org/2005/Atom}author')\
                                        .find('{http://www.w3.org/2005/Atom}name').text,
                           jobPostXmlEle.find('title').text,
                           jobPostXmlEle.find('description').text,
                           jobPostXmlEle.find('pubDate').text, # format example - "Mon, 23 Apr 2018 19:57:45"
                           jobPostXmlEle.find('{http://stackoverflow.com/jobs/}location').text,
                          )
                         )
                         
        # job posting id to create association with its categories
        inserted_job_posting_id = db_cursor.lastrowid

        # for each category job posting is listed under ..
        for category in jobPostXmlEle.iter('category'):
            # .. grab category id from relation 'category', creating a new category row if required
            inserted_category_id = get_category_id(category.text, db_cursor)
            # create association between category & its job posting
            db_cursor.execute('''INSERT INTO job_posting_category VALUES (?, ?)''',\
                                    (inserted_job_posting_id, inserted_category_id,))

    except Exception:
        db_conn.rollback()
        raise
    finally:
        db_conn.commit()

def get_category_id(category_string, db_cursor):
    """If category given by parameter 'category_string' has not been added to
       relation 'category', add it, and then return its 'category_id'. If
       already added to 'category', simply return its 'category_id'.

       If any exception occurs, return 'None'.
    """
    try:
        # fetch category given by param. "category_string" ..
        db_cursor.execute('''SELECT category_id FROM category WHERE category.category_name = (?)''', (category_string,))
        category_row = db_cursor.fetchone()

        # .. if category does not exist, create it
        if category_row == None:
            db_cursor.execute('''INSERT INTO category VALUES (?, ?)''', (None, category_string,))
            category_id = db_cursor.lastrowid
        else:
            category_id = category_row[0]

        return category_id

    except Exception as e:
        raise
        return None

if __name__ == '__main__':
    create_job_posting_relations('qwer.db')
