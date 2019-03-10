import logging
from datamodel.search.Haoc19Mutianx1_datamodel import Haoc19Mutianx1Link, OneHaoc19Mutianx1UnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4
from urlparse import urlparse, parse_qs, urljoin


from uuid import uuid4

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(Haoc19Mutianx1Link)
@GetterSetter(OneHaoc19Mutianx1UnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Haoc19Mutianx1"

    def __init__(self, frame):
        self.app_id = "Haoc19Mutianx1"
        self.frame = frame
        self.num_invalid = 0
        self.invalid_Url = []
        self.maxOut = 0
        self.maxOut_Url = []
        self.subdomain_Map = {}


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneHaoc19Mutianx1UnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = Haoc19Mutianx1Link("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneHaoc19Mutianx1UnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        valid = []
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(Haoc19Mutianx1Link(l))
                    valid.append(l)
                else:
                    self.num_invalid += 1
                    self.invalid_Url.append(l)
            if len(valid) > self.maxOut:
                self.maxOut = len(valid)
                self.maxOut_Url = []
                self.maxOut_Url.append(downloaded.url)
            if len(valid) == self.maxOut:
                self.maxOut_Url.append(downloaded.url)
            l = urlparse(downloaded.url)
            entr = '.'.join(l.hostname.split(".")[:-2])  # delete uci.edu, host name is : subdomain+uci,edu
            if entr in self.subdomain_Map:
                self.subdomain_Map[entr] += 1
            else:
                self.subdomain_Map[entr] = 1
        total = 0
        for numb in self.subdomain_Map.values():
            total += numb

        with open("analysis.txt", 'w') as f:
            f.write("invalid links " + str(self.num_invalid) \
                    + "\nmost out put page:" + str(self.maxOut_Url) \
                    + '\nsubdomain:' + str(self.subdomain_Map) \
                    + '\ntotal crawled page:' + str(total))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []

    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    if rawDataObj.error_message is None:

        htm = etree.HTML(rawDataObj.content)
        if htm is not None:
            r = htm.xpath('//a/@href')
            # r = filter(lambda x: x.startswith('mailto') == False, r)

            r = map(lambda x: urljoin(rawDataObj.url, x) if x.startswith('http') == False else x, r)
            # transfer relative tp absolute form

            r = map(lambda x: x[:-5] if x.endswith('index') == True else x, r)
            r = map(lambda x: x[:-9] if x.endswith('index.php') == True else x, r)
            r = map(lambda x: x[:-1] if x.endswith('/') == True else x, r)
            r = map(lambda x: x[:-4] if x.endswith('.php') == True else x, r)
            r = map(lambda x: x.replace('../', ''), r)

            r = filter(lambda x: x.startswith('http') == True, r)
            # print r
            # with open("output.txt", 'w') as f:
            # f.write("\r\n" + str(r))

            outputLinks = r

    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    if parsed.query != '':  # test connector '&'
        return False
    if 'calendar' in url:
        return False
    r = re.compile(r'~|/zh-tw|/vi|/ko')
    if r.findall(url) != []:
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False

