# -*- coding:utf-8 -*-

# @version: 1.0
# @author: ZhangZhipeng
# @date: 2016-05-05


import re
import os
import sys
from datetime import datetime


import requests
import threadpool
from fuzzywuzzy import fuzz
from pyquery import PyQuery as pq
from multiprocessing import Process, cpu_count
# from multiprocessing import Pool
from pyzipcode import ZipCodeDatabase as ZipDB


SEARCH_URL = "http://www.yelp.com/search"
COMMENT_URL = "http://www.yelp.com/biz/"
SAVE_DIR = "data"
FUZZ_SAME_RATIO = 10
THREAD_COUNT = cpu_count() * 2


# Yelp crawler object - process
class YelpCrawler(Process):

    def __init__(self, search_word, location, job_id, mongo_client=None, matching=True, process_pool=None):
        Process.__init__(self)

        # setting object parameters
        self._search_word = search_word
        self._matching = matching
        self._job_id = job_id
        self._process_pool = process_pool

        self._mongo_client = mongo_client
        self._zip_codes = self._get_zipcode_list(location)
        print "search city list: %s" % " | ".join(self._zip_codes)

        self.store_id_set = set()

    def run(self):
        for location in self._zip_codes:
            if self._process_pool:
                # self._process_pool.apply_async(self._search_loction, (location,))
                self._join_thread(self._search_loction, (location,))
            else:
                self._search_loction(location)

    def _get_zipcode_list(self, location):
        zip_db = ZipDB()
        if re.findall(r'[0-9,]+', location):
            zips = location.split(',')
        elif location.find(",") < 0 and len(location) > 2:
            print "search city ..."
            zips = [z.zip for z in zip_db.find_zip(city=location) or []]
        elif len(location) == 2:
            print "search state ..."
            zips = [z.zip for z in zip_db.find_zip(state=location) or []]
        else:
            print "search city: %s state: %s" % tuple(location.split(","))
            zips = [z.zip for z in zip_db.find_zip(
                city=location.split(',')[0], state=location.split(',')[1]) or []]
        return list(set(["%s,%s" % (zip_db[i].city, zip_db[i].state) for i in zips]))

    def _search_loction(self, location):
        print "search location: %s" % location

        parameters = {"find_desc": self._search_word,
                      "start": 0,
                      "find_loc": location}
        while True:
            res = self._download(SEARCH_URL, params=parameters)
            if not res or res.status_code != 200:
                print "search[%s] error. response status_code: %s" % (self._search_word, res.status_code)
                continue
            store_id_list = self._get_store_ids(res.content.decode("utf8"))
            for store_id in store_id_list:
                # self._process_store_id(store_id)
                if self._process_pool:
                    # self._process_pool.apply_async(self._process_store_id, (store_id,))
                    self._join_thread(self._process_store_id,
                                      ([store_id, location], ))
                else:
                    self._process_store_id([store_id, location])
            if len(store_id_list) < 10:
                break
            parameters["start"] += 10

    # given an HTML page from the search pulls out the store_id
    def _get_store_ids(self, page):
        store_id_list = []
        for i in pq(page)(".biz-name"):
            store_name = i.text_content()
            href = i.get("href")
            store_id = href.split(
                "biz")[-1].split("&")[0].replace("%2F", "").replace("/", "").split("?")[0]
            if self._matching and (not store_name or not self._check_name(store_name)):
                print "[ignore] store_name: %s, id: %s, href: %s" % (store_name, store_id, href)
                continue
            self.store_id_set.add(store_id)
            self._add_id_to_map(store_id)
            store_id_list.append(store_id)
        return store_id_list

    # this is a name check if the _matching field is set - uses fuzzy name
    # _matching within 80% of original string
    def _check_name(self, name):
        store_name = "".join(name.lower().split())
        search_work = "".join(self._search_word.lower().split())

        ratio = fuzz.token_set_ratio(search_work, store_name)
        if ratio >= FUZZ_SAME_RATIO:
            return True
        else:
            return False

    # takes store_id and pulls all the comments
    def _process_store_id(self, store_id_location):
        store_id, location = store_id_location
        print "###" + location + "###" + store_id
        parameters = {"sort_by": "date_desc", "start": 0}
        print "[Yelp] Processing Loc:%s Store ID: %s" % (location, store_id)
        while True:
            url = COMMENT_URL + store_id
            res = self._download(url, params=parameters)
            if not res or res.status_code != 200:
                continue
            comment_list = self._get_comments(res.content.decode("utf8"))
            print "[Yelp] Processing Loc:%s Store ID: %s count: %s" % (location, store_id, len(comment_list))
            if not comment_list:
                break
            for comment in comment_list:
                comment['store_id'] = store_id
                self._insert_review(comment, store_id)
            if len(comment_list) < 20:
                break
            parameters['start'] += 20

    # given an HTML page - parses out commmends and returns
    def _get_comments(self, page):
        page_pq = pq(page)
        # store_location = comment_soup.find('address').text.strip()
        store_location = "\n".join([i.strip() for i in page_pq(
            ".map-box-address")[0].text_content().split("\n") if i.strip()])

        # comments = comment_soup.findAll('div', {'class':'review review--with-sidebar'})
        comment_list = page_pq(
            ".review-list ul li .review.review--with-sidebar")
        result_comment_list = []
        for comment in comment_list:
            com = {}
            comment_id = comment.get('data-review-id')
            if not comment_id:
                continue
            com['id'] = comment_id
            # com['authorID'] = comment.find('a' {"class":'user-display-name'}).text
            # soup_c = BeautifulSoup("""{0}""".format(comment), 'lxml')
            pq_comment = pq(comment)
            com["author_name"] = pq_comment(".user-name").text()
            com["published_date"] = pq_comment(
                ".rating-qualifier meta").attr("content")
            com["author_location"] = pq_comment(".user-location").text()
            com["store_location"] = store_location
            com["author_img_url"] = "http:" + \
                pq_comment(".photo-box img").attr("src")
            comment = pq_comment('[@itemprop="description"]').text()
            comment = re.sub(" {2,}", " ", re.sub(r"&\w+;", " ", comment))
            comment = comment.replace("\t", ". ").replace(
                "\n", ". ").replace(".. ", ". ").strip()
            com["text"] = comment
            result_comment_list.append(com)
        return result_comment_list

    # keeps track of all store_ids based on the search (used on signals)
    def _add_id_to_map(self, sid):
        self._mongo_client["yelp_data"]["job_map"].update({"_id": self._job_id},
                                                          {"$addToSet": {"store_id_list": sid}}, upsert=True)

    # inserts review into store
    def _insert_review(self, review, store_id):
        review["_id"] = review["id"]
        if self._mongo_client:
            try:
                self._mongo_client["yelp_data"][store_id].update(
                    {"_id": review["_id"]}, review, upsert=True)
                return True
            except Exception as e:
                print "Save MongoDB Error:%s, store ID:%s, reviews: %s", (e, store_id, review)
                return False
        return True

    def _join_thread(self, fun, params):
        for req in threadpool.makeRequests(fun, params):
            self._process_pool.putRequest(req)

    def _download(self, url, params, try_num=5, timeout=30, *args, **kwargs):
        while try_num:
            try:
                res = requests.get(url, params=params, *args, **kwargs)
                return res
            except requests.exceptions, e:
                print "[%s]Download error: %s, url: %s" % (try_num, url, e)
                try_num -= 1
        return None
        # for i in self.store_id_set:
        #     self.process_id(i)

        # self.write_csv()

    def write_csv(self):
        docs = []
        import csv
        for i in self.store_id_set:
            docs.extend(list(self._mongo_client.yelp_data[i].find()))

        write_header = False
        try:
            os.makedirs(SAVE_DIR)
        except (IOError, WindowsError):
            pass
        file_path = os.path.join(SAVE_DIR, self._search_word + '.csv')
        print "save file: [%s]" % file_path
        try:
            with file(file_path, "w")as f:
                f.write("")
        except IOError, e:
            print "save file: %s error: %s" % (file_path, e)
            file_path = os.path.join(SAVE_DIR, self._search_word + str(datetime.now())[:19] + '.csv')
            print "try save file: [%s]" % file_path

        with file(file_path, 'w') as f:
            for store_id in self.store_id_set:
                for review_data in list(self._mongo_client.yelp_data[store_id].find()):
                    if not write_header:
                        csv_wf = csv.DictWriter(f, review_data.keys())
                        csv_wf.writeheader()
                        write_header = True
                    for k in review_data.keys():
                        review_data[k] = review_data[k].encode('utf8', errors="ignore")
                    csv_wf.writerow(review_data)

if __name__ == "__main__":

    """ Crawler takes
        str         search_term : the search you would like to find
        str         location : single zipcode or comma seperated zipcodes 28202,28203
        str         job_id = used by signals, passed in to attached all store_id's for csv rebuild
        MongoClient mongo_client = client used to insert reviews
    """

    dump_file = True if "-d" in sys.argv else False

    from pymongo import MongoClient

    client = MongoClient('127.0.0.1')
    # client = MongoClient('192.168.0.4')

    # process_pool = Pool(processes=1)
    process_pool = threadpool.ThreadPool(THREAD_COUNT)

    # yelp_crawler = YelpCrawler("mcdonalds", 'CA', 'test', mongo_client=client)
    # yelp_crawler = YelpCrawler("mcdonalds", 'CA', 'test', mongo_client=client, process_pool=process_pool)

    yelp_crawler = YelpCrawler(
        "mcdonalds", '28202', 'test', mongo_client=client, process_pool=process_pool)
    # yelp_crawler = YelpCrawler("mcdonalds", 'Charlotte', 'test', mongo_client=client, process_pool=process_pool)
    yelp_crawler.run()
    process_pool.wait()
    if dump_file:
        yelp_crawler.write_csv()

    # for i in store:
    #     yelp_crawler.store_id_set.add(i)
    #     yelp_crawler.process_id(i)

    # yelp_crawler.write_csv()
    # yelp_crawler.process_id('peets-coffee-and-tea-seattle')

    # # YC3 = YelpCrawler('mcdonalds', 'NC')

    # yelp_crawler.run()
