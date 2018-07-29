"""When run, program pulls xml from a Stack Overflow rss feed related to job
   openings. Job posting data is stored in an sqlite3 database.

   Rss feed address: 'https://stackoverflow.com/jobs/feed?location=melbourne'

   All job posting data obtained from Stack
   Overflow ('https://stackoverflow.com/'').
   This program is intended for non-commercial purposes.
"""

import dataOps
import xml.etree.ElementTree as ET
import urllib.request

URL = 'https://stackoverflow.com/jobs/feed?location=melbourne'

def main():
    """Grab xml from rss feed at 'URL'. Use python module 'ElementTree' to
       traverse tree, and then insert each job posting into relations
       in database passed as argument to 'dataOps.insert_job_posting()'
    """
    webString = getStringFromUrl(URL)
    root = ET.fromstring(webString) # grab root element of tree of xml elements

    db_conn = dataOps.get_db_connect('qwer.db')

    for jobPostingItem in root[0].iter('item'):
        dataOps.insert_job_posting(jobPostingItem, db_conn)

    db_conn.close()

def getStringFromUrl(url):
    """Given a url, returns string of data at url.
    """
    file = urllib.request.urlopen(url)
    data = file.read()
    file.close()

    return data

if __name__ == '__main__':
    main()
