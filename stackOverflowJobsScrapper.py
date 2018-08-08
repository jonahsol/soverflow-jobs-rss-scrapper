"""When run, program pulls xml from a Stack Overflow rss feed related to job
   openings. Job posting data is stored in an sqlite3 database.

   Example rss feed address: 'https://stackoverflow.com/jobs/feed?location=melbourne'

   All job posting data obtained from Stack
   Overflow ('https://stackoverflow.com/'').
   This program is intended for non-commercial purposes.
"""

import dataOps
import xml.etree.ElementTree as ET
import urllib.request

rssFeedUrlBeginning = 'https://stackoverflow.com/jobs/feed?location='

def main():
    locations = ['Melbourne'
                 'Sydney',
                 'America',
                 'England',
                 'Germany',
                 'Japan',
                 'Sweden',
                 'Canada',
                 'Belgium']

    for location in locations:
        pullJobPostingsAtLocation(location, 'production(8.8.18).db')

def pullJobPostingsAtLocation(location, database_name_string):
    """Given a location, pulls job posting xml data from rss feed url (see file level
       docstring for example), and then inserts each job posting into db
       specified by param. 'database_name_string'.

       Note that any non empty string for param. 'location' is a valid
       url - eg. "rssFeedUrlBeginning + 'a'" is a valid url
    """
    # grab xml from stackoverflow rss feed
    webString = get_string_from_url(rssFeedUrlBeginning + location)
    # grab root element of tree of xml elements
    root = ET.fromstring(webString)

    db_conn = dataOps.get_db_connect(database_name_string)

    rss_urls_hash = {}

    # for each job posting in the rss feed, insert into database - concurrently,
    # insert the url of that job posting into 'rss_urls_hash'
    for jobPostingItem in root[0].iter('item'):
        rss_urls_hash[dataOps.insert_job_posting(jobPostingItem, location, db_conn)] = None

    # compare urls just scrapped to urls already in the db, adding an end date
    # to a db url if required
    dataOps.add_close_dates(location, db_conn, rss_urls_hash)

    db_conn.close()

def get_string_from_url(url):
    """Given a url, returns string of data at url.
    """
    file = urllib.request.urlopen(url)
    data = file.read()
    file.close()

    return data

if __name__ == '__main__':
    main()
