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

def main():
    """Grab xml from rss feed at 'URL'. Use python module 'ElementTree' to
       traverse tree, and then insert each job posting into relations
       in database passed as argument to 'dataOps.insert_job_posting()'
    """
    dataOps.pullJobPostingsAtLocation('melbourne', 'qwer.db')

def getStringFromUrl(url):
    """Given a url, returns string of data at url.
    """
    file = urllib.request.urlopen(url)
    data = file.read()
    file.close()

    return data

if __name__ == '__main__':
    main()
