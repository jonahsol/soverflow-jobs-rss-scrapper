"""dataOps.py deals with operations pertaining to sqlite3 database for stack overflow
   scrapper in 'stackOverflowJobsScrapper.py'.
   Running 'dataOps' as main calls 'create_job_posting_relations(..)', initialising the db.

   db relations:

   job_posting -
   PK(job_posting_id) | URL | author | title | description | pub_date | close_date | job_post_location | rss_location

   category -
   PK(category_id) | category_name

   job_posting_category -
   PK/FK(job_posting_id) | PK/FK(category_id)
"""

import sqlite3 as sql
from datetime import datetime
import sys

def main():
    create_job_posting_relations('production(8.8.18).db')

def get_db_connect(database_name_string):
    """Return sqlite3 connection to db specified by param. 'database_name_string'
    """
    try:
        db = sql.connect(database_name_string)
        return db

    except Exception as e:
        # the following is used as an alternative to 'raise', which does not behave well with sqlite3 errors - see https://stackoverflow.com/questions/25371636/how-to-get-sqlite-result-error-codes-in-python
        curFuncNameAsString = sys._getframe().f_code.co_name # attribution: https://stackoverflow.com/questions/5067604/determine-function-name-from-within-that-function-without-using-traceback
        print("Error in function: " + curFuncNameAsString + "-")
        print(e)

def create_job_posting_relations(new_database_name_string):
    """Create db and initialise relations to store job posting data. Db has
       structure shown in docstring at top of file.
    """
    # when making a connection, sqlite3 will create a new db, if
    # 'new_database_name_string' does not already exist.
    db_conn = get_db_connect(new_database_name_string)
    db_cursor = db_conn.cursor()

    try:
        # job posting relation
        #
        # rss_location - location for rss feed, specified in rss feed url
        # job_post_location - location specified by the xml element of a given job posting;
        #                     what is seen when looking at a job posting on SOverflow website
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS job_posting (
                                        job_posting_id INTEGER PRIMARY KEY,
                                        URL TEXT,
                                        author TEXT,
                                        title TEXT,
                                        description TEXT,
                                        pub_date TEXT,
                                        close_date TIMESTAMP,
                                        job_post_location TEXT,
                                        rss_location TEXT
                                        )''')

        # category relation
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS category (
                                    category_id INTEGER PRIMARY KEY,
                                    category_name TEXT
                                    )''')

        # association table between 'category' & 'job_posting'
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS job_posting_category (
                                    job_posting_id INTEGER,
                                    category_id INTEGER,
                                    FOREIGN KEY (job_posting_id)
                                    REFERENCES job_posting(job_posting_id),
                                    FOREIGN KEY (category_id)
                                    REFERENCES category(category_id),
                                    PRIMARY KEY (job_posting_id, category_id)
                                    )''')

        # index on 'job_posting.URL'
        db_cursor.execute('''CREATE INDEX job_posting_urls ON job_posting.URL''')

    except Exception as e:
        # the following is used as an alternative to 'raise', which does not behave well with sqlite3 errors - see https://stackoverflow.com/questions/25371636/how-to-get-sqlite-result-error-codes-in-python
        curFuncNameAsString = sys._getframe().f_code.co_name # attribution: https://stackoverflow.com/questions/5067604/determine-function-name-from-within-that-function-without-using-traceback
        print("Error in function: " + curFuncNameAsString + "-")
        print(e)

        db_conn.rollback()


    finally:
        db_conn.commit()
        db_conn.close()

def insert_job_posting(job_post_xml_ele, rss_location, db_conn):
    """Given a job posting in a 'xml.etree.ElementTree.Element', insert relevant
       data into database specified by param. 'db_conn'.
    """
    curs = db_conn.cursor()
    try:
        job_post_url = job_post_xml_ele.find('link').text
        job_post_in_db = curs.execute('''SELECT URL FROM job_posting WHERE URL = (?)''',
                                        (job_post_url, )).fetchall()

        if not job_post_in_db:
            # insert row into relation 'job_posting'
            curs.execute('''INSERT INTO job_posting VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (None,
                               job_post_url,
                               job_post_xml_ele.find('{http://www.w3.org/2005/Atom}author')\
                                            .find('{http://www.w3.org/2005/Atom}name').text,
                               job_post_xml_ele.find('title').text,
                               job_post_xml_ele.find('description').text,
                               job_post_xml_ele.find('pubDate').text, # format example - "Mon, 23 Apr 2018 19:57:45"
                               None, # close date empty for now
                               job_post_xml_ele.find('{http://stackoverflow.com/jobs/}location').text,
                               rss_location,
                              )
                             )

            # add job postings categories

            # job posting id to create association with its categories
            inserted_job_posting_id = curs.lastrowid
            for category in job_post_xml_ele.iter('category'):
                # .. grab category id from relation 'category', creating a new category row if required
                inserted_category_id = get_category_id(category.text, curs)
                # create association between category & its job posting
                curs.execute('''INSERT INTO job_posting_category VALUES (?, ?)''',\
                                        (inserted_job_posting_id, inserted_category_id, ))

    except Exception as e:
        # the following is used as an alternative to 'raise', which does not behave well with sqlite3 errors - see https://stackoverflow.com/questions/25371636/how-to-get-sqlite-result-error-codes-in-python
        curFuncNameAsString = sys._getframe().f_code.co_name # attribution: https://stackoverflow.com/questions/5067604/determine-function-name-from-within-that-function-without-using-traceback
        print("Error in function: " + curFuncNameAsString + "-")
        print(e)

        db_conn.rollback()

    finally:
        db_conn.commit()
        return job_post_url

def get_category_id(category_string, db_cursor):
    """If category given by parameter 'category_string' has not been added to
       relation 'category', add it, and then return its 'category_id'. If
       already added to 'category', simply return its 'category_id'.

       If any exception occurs, return 'None'.
    """
    try:
        # fetch category given by param. "category_string" ..
        db_cursor.execute('''SELECT category_id FROM category WHERE category.category_name = (?)''', (category_string, ))
        category_row = db_cursor.fetchone()

        # .. if category does not exist, create it
        if category_row == None:
            db_cursor.execute('''INSERT INTO category VALUES (?, ?)''', (None, category_string, ))
            category_id = db_cursor.lastrowid
        else:
            category_id = category_row[0]

        return category_id

    except Exception as e:
        # the following is used as an alternative to 'raise', which does not behave well with sqlite3 errors - see https://stackoverflow.com/questions/25371636/how-to-get-sqlite-result-error-codes-in-python
        curFuncNameAsString = sys._getframe().f_code.co_name # attribution: https://stackoverflow.com/questions/5067604/determine-function-name-from-within-that-function-without-using-traceback
        print("Error in function: " + curFuncNameAsString + "-")
        print(e)

        return None

def add_close_dates(rss_location, db_conn, rss_urls_hash):
    """TL;DR - For a given location, if there is a job posting in the db, but
               it is not in the rss feed, add an end date to that job posting.

       Note that close dates update to the datetime of when the scrapper notices
       the job posting is no longer present - this may not at all reflect the
       actual time the job posting disappeared on the sOverflow website.

       Given a hash of all job posting urls ('rss_urls_hash') at a given
       rss feed location ('location'), compare these urls to the urls already
       in the database. If there is a url in the database that is not in the hash,
       then a job posting has been removed from that stack overflow rss feed.
       So update 'close_date' attribute for that job posting.
    """
    curs = db_conn.cursor()
    try:
        # grab all urls from db at location specified by param. 'rss_location',
        # that aren't already closed
        db_urls = (curs.execute('''SELECT URL FROM job_posting
                                   WHERE rss_location = (?) AND (close_date IS NULL)''',
                                   (rss_location, ))
                                   .fetchall())

        # if a db url is in 'db_urls' but not param. 'rss_urls_hash', update attrib.
        # 'close_date'
        for db_url_tup in db_urls:
            if not (db_url_tup[0] in rss_urls_hash):
                curs.execute('''UPDATE job_posting SET close_date = (?) WHERE URL = (?)
                             ''', (datetime.now(), db_url_tup[0], ))
    except Exception as e:
        # the following is used as an alternative to 'raise', which does not behave well with sqlite3 errors - see https://stackoverflow.com/questions/25371636/how-to-get-sqlite-result-error-codes-in-python
        curFuncNameAsString = sys._getframe().f_code.co_name # attribution: https://stackoverflow.com/questions/5067604/determine-function-name-from-within-that-function-without-using-traceback
        print("Error in function: " + curFuncNameAsString + "-")
        print(e)

        db_conn.rollback()

    finally:
        db_conn.commit()

if __name__ == '__main__':
    main()
