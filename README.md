# Project Title
A small script to collate data from the [Stack Overflow jobs rss feed](https://stackoverflow.com/jobs/feed?location=melbourne). This should be stored in a database, and data should be analysed over (at least) the technologies used in jobs.

### Prerequisites

 - Python

### Usage

 - Running 'dataOps.py' as main will initialise a db for job posting relations. This can be
   done to create a fresh db (see func. 'create_job_posting_relations()' in 'dataOps.py'
 - Running 'stackOverflowJobsScrapper' as main will pull xml from rss feed at specified locations,
   and insert job posting data into the specified database.
 - Querying database - see https://docs.python.org/2/library/sqlite3.html

### TODO

* Do some data analysis

### Attribution

Thanks to Stack Overflow for job posting data. This project is intended for
non-commercial purposes.
