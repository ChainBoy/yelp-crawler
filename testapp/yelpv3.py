from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
from time import sleep
from multiprocessing import Process
from pyzipcode import *
from pyquery import PyQuery as pq

import requests
import regex
import random
import sys

SEARCH_URL = "http://www.yelp.com/search"
COMMENT_URL = "http://www.yelp.com/biz/"


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
        store_name = "".join(name.lower().split())
        search_work = "".join(self.searchterm.lower().split())

        ratio = fuzz.token_set_ratio(search_work, store_name)
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
        store_id_list = []
        for i in pq(page)(".biz-name"):
            store_name =  i.text_content()
            href = i.get('href')
            store_id = href.split("biz")[-1].split("&")[0].replace("%2F","").replace("/", "").split("?")[0]
            if self.matching and (not store_name or  not self.check_name(store_name)):
                print "error: store_name: %s, id: %s, href: %s" % (store_name, store_id, href)
                continue
            self.store_id_set.add(store_id)
            self.add_id_to_map(store_id)
            store_id_list.append(store_id)
        return store_id_list


    # given an HTML page - parses out commmends and returns
    def get_comments(self, page):
        page_pq = pq(page)
        comment_soup = BeautifulSoup(page, 'html.parser')

        # store_location = comment_soup.find('address').text.strip()
        store_location = "\n".join([i.strip() for i in page_pq(".map-box-address")[0].text_content().split("\n") if i.strip()])

        # comments = comment_soup.findAll('div', {'class':'review review--with-sidebar'})
        comment_list = page_pq(".review-list ul li .review.review--with-sidebar")

        result_comment_list = []
        for comment in comment_list:
            try:
                com = {}
                comment_id = comment.get('data-review-id')
                if  not comment_id:
                    continue
                com['id'] = comment_id
                # com['authorID'] = comment.find('a' {"class":'user-display-name'}).text
                # soup_c = BeautifulSoup("""{0}""".format(comment), 'lxml')
                com['author_name'] = pq(comment)(".user-name").text()
                com['published_date'] = pq(comment)(".rating-qualifier meta").attr("content")
                com['author_location'] = pq(comment)(".user-location").text()
                com['store_location'] = store_location
                com['text'] = pq(comment)('[@itemprop="description"]').text().replace("\t", ". ").replace("\n", ". ").replace(".. ",". ").strip()
                com['author_img_url'] = 'http:'+pq(comment)(".photo-box img").attr("src")
                result_comment_list.append(com)
            except Exception as e:
                print "Failed: ", e
        return result_comment_list

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
                url = COMMENT_URL + store_id
                res = requests.get(url, params=parameters)
                if res.status_code == 200:

                    comment_list = self.get_comments(res.content)
                    if not comment_list:
                        break
                    for comment in comment_list:
                        comment['store_id'] = store_id
                        if not self.insert_review(comment, store_id): return
                    if len(comment_list) < 20:
                        break
                    parameters['start'] += 20
                sleep(5)
            except Exception as e:
                print e
                break


    def run(self):
        for location in self.zips:
            parameters = {'find_desc' : self.searchterm, 'start' : 0, 'find_loc': location}
            while True:
                try:
                    res = requests.get(SEARCH_URL, params=parameters, allow_redirects=True)

                    if res.status_code == 200:
                        store_id_list = self.get_store_ids(res.content)
                        for store_id in store_id_list:
                            print "get store_id: %s comment." % store_id
                            self.process_id(store_id)
                        if len(store_id_list) < 10:
                            break
                        parameters['start'] += 10
                    else:
                        print res.status_code
                        # print res.content
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









