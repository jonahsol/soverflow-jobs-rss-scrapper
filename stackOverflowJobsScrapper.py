"""When run, program pulls xml from a Stack Overflow rss feed related to job
   openings. Job posting data collated, so we can do interesting things with
   it.

   Rss feed address: 'https://stackoverflow.com/jobs/feed?location=melbourne'

   All job posting data obtained from Stack
   Overflow ('https://stackoverflow.com/'').
   This program is intended for non-commercial purposes.
"""

import xml.etree.ElementTree as ET
import urllib.request
from collections import Counter

URL = 'https://stackoverflow.com/jobs/feed?location=melbourne'


def main():
    """Grab xml from rss feed at 'URL'. Use python module 'ElementTree' to grab
       job posting xml elements from this feed, and then store these xml
       elements in instantiations of class 'JobPosting'.
    """

    webString = getStringFromUrl(URL)
    root = ET.fromstring(webString) # grab root element of tree of xml elements

    # extract info from xml elements containing job postings - each job posting
    # goes into an element of 'job_postings'
    job_postings = [JobPosting(item) for item in root[0].iter('item')]
    for job_posting in job_postings:
        job_posting.extractInterestingInfo()

    # grab data for analytics from current job postings
    categories_dict = JobData.extractCategories(job_postings)

    #printWordCloud(" ".join()job)


def getStringFromUrl(url):
    """Given a url, returns string of data at url.
    """

    file = urllib.request.urlopen(url)
    data = file.read()
    file.close()

    return data


class JobPosting(object):
    """Handles extraction of interesting data from a job posting element in
       the Stack Overflow jobs rss.
    """

    def __init__(self, jobPostXmlEle):
        self.xmlEle = jobPostXmlEle

    def extractInterestingInfo(self):
        self.title = self.xmlEle.find('title').text
        self.categories = [category.text for category in
                                                self.xmlEle.iter('category')]

        self.description = self.xmlEle.find('description').text
        # format example - "Mon, 23 Apr 2018 19:57:45"
        self.pubDate = self.xmlEle.find('pubDate').text
        self.location = self.xmlEle.find('{http://stackoverflow.com/jobs/}location').text

    def printJobPosting(self):
        print("Title:", self.title)
        print(self.categories)
        print(self.pubDate)
        print(self.location)


class JobData(object):
    """Records and stores data about all job postings from the rss feed.
    """

    #def __init__(self):
        #self.categoryDict = Counter()

    def extractCategories(jobPostingList):
        """
        """

        category_counter = Counter()
        for jobPosting in jobPostingList:
            for category in jobPosting.categories:
                category_counter[category] += 1

        return category_counter


def printWordCloud(text):
   """Attribution: 'https://github.com/amueller/word_cloud'
   """

    from wordcloud import  WordCloud
    wordcloud = WordCloud(max_font_size=40).generate(text)

    import matplotlib.pyplot as plt
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')


if __name__ == '__main__':
    main()
