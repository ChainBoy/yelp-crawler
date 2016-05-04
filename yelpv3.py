from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz, process
from nltk.stem.porter import PorterStemmer
from urlparse import urljoin
from time import sleep
from multiprocessing import Process
from pyzipcode import *

import requests
import regex
import random
import sys


### Yelp crawler object - process
class YelpCrawler(Process):
    def __init__(self, searchterm, location, job_id, mongo_client=None, matching=True, search_parameters={}, search_country=False):
        Process.__init__(self)

        #setting object parameters
        self.searchterm = searchterm
        self.location = location
        self.matching = matching
        self.job_id = job_id

        self.mongo_client = mongo_client

        self.stemmer = PorterStemmer()


        if regex.match(r'[0-9,]+', location):
            self.zips = location.split(',')
        elif len(location) == 2:
            self.zips = [z.zip for z in ZipCodeDatabase().find_zip(state=location)]
        else:
            self.zips = [z.zip for z in ZipCodeDatabase().find_zip(city = location.split(',')[0], state=location.split(',')[1])]

        city_set = set()
        zdb = ZipCodeDatabase()
        for i in self.zips:
            try:
                if True:
                    city_set.add("%s,%s" % (zdb[i].city, zdb[i].state))
            except:
                pass

        self.zips = list(city_set)
        self.store_id_set = set()

    # this is a name check if the matching field is set - uses fuzzy name matching within 80% of original string
    def check_name(self, name):
        title_tokens = name.lower().split()
        search_tokens = self.searchterm.lower().split()
        stemmed_title_string = ' '.join([self.stemmer.stem(x) for x in title_tokens])
        stemmed_search_string = ' '.join([self.stemmer.stem(x) for x in search_tokens])

        ratio = fuzz.token_set_ratio(stemmed_search_string, stemmed_title_string)
        # print name.encode('utf8'), ratio
        if ratio >= 80:
            return True
        else:
            return False

    # keeps track of all store_ids based on the search (used on signals)
    def add_id_to_map(self, sid):
        self.mongo_client['yelp_data']['job_map'].update({'_id':self.job_id}, 
            {'$addToSet':{"store_id_list":sid}}, upsert=True)


    # given an HTML page from the search pulls out the store_id
    def get_store_ids(self, page):
        soup = BeautifulSoup(page, 'html.parser')
        while True:
            ids = soup.findAll('a', {'class':'biz-name'})
            if not ids:
                print "FACK"
            break


        for i in ids:
            id_str = i._attr_value_as_string('href')
            
            if '/biz/' in id_str:
                if self.matching:
                    store_name = i.text
                    # store_name = i.find('span', class_="biz-name js-analytics-click"})

                    if store_name != None and self.check_name(store_name):

                        clean_str = regex.search(r'biz\/([0-9a-z\-]+)(\?|$)', id_str)[1]
                        if clean_str not in self.store_id_set:
                            self.store_id_set.add(clean_str)
                            self.add_id_to_map(clean_str)
                            yield clean_str
                else:
                    clean_str = regex.search(r'biz\/([0-9a-z\-]+)(\?|$)', id_str)[1]
                    if clean_str not in self.store_id_set:
                        self.store_id_set.add(clean_str)
                        self.add_id_to_map(clean_str)
                        yield clean_str

    # given an HTML page - parses out commmends and returns
    def get_comments(self, page):
        comment_soup = BeautifulSoup(page, 'html.parser')

        store_location = comment_soup.find('address').text.strip()

        comments = comment_soup.findAll('div', {'class':'review review--with-sidebar'})

        clean_comments = []
        for c in comments:
            try:
                com = {}
                if c.get('data-review-id') == None:
                    continue
                com['id'] = c.get('data-review-id')
                # com['authorID'] = c.find('a' {"class":'user-display-name'}).text
                # soup_c = BeautifulSoup("""{0}""".format(c), 'lxml')
                
                
                com['author_name'] = c.find('a', {"class":'user-display-name'}).text

                com['published_date'] = c.find('meta', itemprop='datePublished')._attr_value_as_string('content')

                com['author_location'] = c('b')[0].next

                com['store_location'] = store_location

                com['text'] = c('p')[0].next

                com['author_img_url'] = 'http:'+c('img')[0]['src']

                
                clean_comments.append(com)
            except Exception as e:
                print "Failed: ", e
        
        # print len(clean_comments)
        # print clean_comments
        return clean_comments

    # inserts review into store
    def insert_review(self, review, store_id):
        review['_id'] = review['id']
        if self.mongo_client:
            try:
                self.mongo_client['yelp_data'][store_id].update({'_id':review['_id']}, review, upsert=True)
                return True
            except Exception as e:
                print e
                print "Found all these from store ID: ", store_id
                return False
        return True

    #takes store_id and pulls all the comments
    def process_id(self, store_id):
        parameters = {'sort_by' : 'date_desc', 'start':0}
        print "[Yelp] Processing Store ID: ", store_id
        while True:
            try:
                url = 'http://www.yelp.com/biz/' + store_id
                r = requests.get(url, parameters)
                if r.status_code == 200:

                    comments = self.get_comments(r.content)
                    if len(comments) > 0:
                        for c in comments:
                            c['store_id'] = store_id
                            if not self.insert_review(c, store_id): return
                        if len(comments) < 20:
                            break
                        parameters['start'] += 20
                    else:
                        break
                sleep(5)
            except Exception as e:
                print e
                break

    
    def run(self):
        url = 'www.yelp.com/search'
        parameters = {'find_desc' : self.searchterm, 'start' : 0}

        for z in self.zips[0:1000]:
            # print z
            # print "%s/%s" % (self.zips.index(z), len(self.zips[0:1000]))
            # print len(self.store_id_set)
            parameters['start'] = 0
            parameters['find_loc'] = z
            while True:
                try:
                    url = 'http://www.yelp.com/search'

                    r = requests.get(url, params=parameters, allow_redirects=True)

                    if r.status_code == 200:
                        for i in self.get_store_ids(r.content):
                            print i
                            self.process_id(i)

                        if parameters['start'] <= 10:
                            parameters['start'] += 10
                            # print parameters['start']
                        else:
                            break
                    else:
                        print r.status_code
                        # print r.content
                        sleep(10)   
                except Exception as e:
                    print e
                    break
            # sleep()

        # for i in self.store_id_set:
        #     self.process_id(i)

        # self.write_csv()

    # def write_csv(self):
    #     docs = []
    #     import csv
    #     for i in self.store_id_set:
    #         docs.extend(list(self.mongo_client.yelp_data[i].find()))

    #     with open(self.searchterm + '.csv', 'w') as f:
    #         headers = docs[0].keys()
    #         w = csv.DictWriter(f, headers)
    #         w.writeheader()
    #         for d in docs:
    #             for k in d.keys():
    #                 try:
    #                     d[k] = d[k].encode('utf8')
    #                 except:
    #                     pass
    #         w.writerow(d)

if __name__=="__main__":

    """ Crawler takes
        str         search_term : the search you would like to find
        str         location : single zipcode or comma seperated zipcodes 28202,28203
        str         job_id = used by signals, passed in to attached all store_id's for csv rebuild
        MongoClient mongo_client = client used to insert reviews
    """


    from pymongo import MongoClient

    client = MongoClient('192.168.0.4')

    YC2 = YelpCrawler("mcdonalds", '28202', 'test', mongo_client=client)
    YC2.start()
    # for i in store:
    #     YC2.store_id_set.add(i)
    #     YC2.process_id(i)

    # YC2.write_csv()
    # YC2.process_id('peets-coffee-and-tea-seattle')

    # # YC3 = YelpCrawler('mcdonalds', 'NC')

    # YC2.run()








 
