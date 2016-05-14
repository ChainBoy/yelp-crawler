# -*- coding:utf-8 -*-

# @version: 1.0
# @author: ZhangZhipeng
# @date: 2016-05-05


import re
import os
import sys
import socket
from datetime import datetime

from multiprocessing import Process, cpu_count, Pool, freeze_support, Queue
# from multiprocessing import Pool

import requests
import threadpool
from fuzzywuzzy import fuzz
from pymongo import MongoClient
from pyquery import PyQuery as pq
from pyzipcode import ZipCodeDatabase as ZipDB


from proxy_manager import get_proxy_manager

LOCATION = "all"
LOCATION = "28202"
TASK_NAME = "test"

SEARCH_URL = "http://www.yelp.com/search"
COMMENT_URL = "http://www.yelp.com/biz/"
SAVE_DIR = "data"
FUZZ_SAME_RATIO = 10
THREAD_COUNT = cpu_count() * 2


MONGO_CLIENT = None


# Yelp crawler object - process
class YelpCrawler(Process):

    def __init__(self, search_word, location, job_id, mongo_client=None, matching=True, thread_pool=None, crawler_category=False):
        """if crawler_all is True, search_word mean is category(cflt).
        if location is all, will search all USA location.
        """
        Process.__init__(self)

        # setting object parameters
        self._search_word = search_word
        self._matching = matching
        self._job_id = job_id
        self.threadpool = threadpool

        if type(thread_pool) == int:
            thread_pool = self.threadpool.ThreadPool(thread_pool)
        self._thread_pool = thread_pool

        if not mongo_client:
            mongo_client = get_mongo_client()
        self._mongo_client = mongo_client

        self._crawler_category = crawler_category

        self._zip_codes = self._get_zipcode_list(location)
        print "search city list: %s" % " | ".join(self._zip_codes)

        self.store_id_set = set()
        self._proxy_manager = get_proxy_manager()

    def run(self):
        for location in self._zip_codes:
            if self._thread_pool:
                # self._thread_pool.apply_async(self._search_loction, (location,))
                self._join_thread(self._search_loction, (location,))
            else:
                self._search_loction(location)
        self._thread_pool.wait()

    def _get_zipcode_list(self, location):
        if location.lower() == "all":
            with file("yelp-city-USA-list.txt")as f:
                return f.read().split()
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
        print "===============> search location: %s" % location

        parameters = {"find_desc" if not self._crawler_category else "cflt": self._search_word,
                      "start": 0,
                      "find_loc": location}
        print parameters

        while True:
            res = self._download(SEARCH_URL, params=parameters)
            if not res or res.status_code != 200:
                print "search[%s] error. response status_code: %s" % (self._search_word, res.status_code)
                continue
            store_id_list = self._get_store_ids(res.content.decode("utf8"))
            for store_id in store_id_list:
                # self._process_store_id(store_id)
                if self._thread_pool:
                    # self._thread_pool.apply_async(self._process_store_id, (store_id,))
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
        try:
            store_location = "\n".join([i.strip() for i in page_pq(
                ".map-box-address")[0].text_content().split("\n") if i.strip()])
        except IndexError, e:
            print "why index error?"
            with file("200-index-error.html", "w")as f:
                f.write(page.encode("utf8"))

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
        print "join thread: %s %s" % (fun, params)
        for req in self.threadpool.makeRequests(fun, params):
            self._thread_pool.putRequest(req)

    def _download(self, url, params, try_num=10, timeout=30, *args, **kwargs):
        while try_num:
            try:
                proxy = self._proxy_manager.get()
                if not proxy:
                    proxies = None
                else:
                    proxies={"http": "http://"+proxy}
                res = requests.get(url, params=params, proxies=proxies, timeout=timeout, *args, **kwargs)
                if res.status_code in (503, 404, 403):
                    print "[!%s]proxy download proxy: %s, url: %s, proxy: %s" % (res.status_code, proxy, url, proxy)
                    with file(str(res.status_code) + ".html", "w")as f:
                        f.write(res.content)
                    self._proxy_manager.remove(proxy)
                    continue
                return res
            except (requests.exceptions, requests.RequestException, socket.timeout), e:
                print "[%s]Download error: %s, url: %s proxy: %s" % (try_num, e, url, proxy)
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

def _search_by_category(task_queue, location="CA", task_name="test", dump_file=False):
    print "task_queue", task_queue
    print "location", location
    print "task_name", task_name
    print "dump_file", dump_file

    while not task_queue.empty():
        category = task_queue.get()
        yelp_crawler = YelpCrawler(category, location, task_name, mongo_client=None, thread_pool=THREAD_COUNT, crawler_category=True, matching=False)
        yelp_crawler.run()
        if dump_file:
            yelp_crawler.write_csv()
    return True

def get_mongo_client():
    client = MongoClient('127.0.0.1')
    return client

def search_by_category(location, task_name, dump_file=False):
    process_pool = Pool(processes=1)

    category_queue = Queue()
    with file("yelp-category-level2.txt")as f:
        for i in f.read().split():
            category_queue.put(i)

    process_list = []
    for i in range(cpu_count()):
        process_list.append(Process(target=_search_by_category, args=(category_queue, location, task_name, dump_file)))
    for i in process_list:
        i.start()
    for i in process_list:
        i.join()


def search_by_normal(location, task_name="test", mongo_client=None, dump_file=False):
    mongo_client = mongo_client if mongo_client else get_mongo_client()
    thread_pool = threadpool.ThreadPool(THREAD_COUNT)
    yelp_crawler = YelpCrawler("mcdonalds", location, task_name, mongo_client=mongo_client, thread_pool=thread_pool)
    yelp_crawler.run()
    thread_pool.wait()
    if dump_file:
        yelp_crawler.write_csv()

if __name__ == "__main__":

    """ Crawler takes
        str         search_term : the search you would like to find
        str         location : single zipcode or comma seperated zipcodes 28202,28203
        str         job_id = used by signals, passed in to attached all store_id's for csv rebuild
        MongoClient mongo_client = client used to insert reviews
    """
    freeze_support()

    dump_file = True if "-d" in sys.argv else False
    search_all = True if "--all" in sys.argv else False


    if search_all:
        search_by_category(LOCATION, TASK_NAME, dump_file=dump_file)
    else:
        client = get_mongo_client()
        search_by_normal(LOCATION, TASK_NAME, mongo_client=client, dump_file=dump_file)

    # yelp_crawler = YelpCrawler("mcdonalds", 'CA', 'test', mongo_client=client)
    # yelp_crawler = YelpCrawler("mcdonalds", 'CA', 'test', mongo_client=client, thread_pool=thread_pool)

    # yelp_crawler = YelpCrawler(
    #     "mcdonalds", '28202', 'test', mongo_client=client, thread_pool=thread_pool)
    # # yelp_crawler = YelpCrawler("mcdonalds", 'Charlotte', 'test', mongo_client=client, thread_pool=thread_pool)
    # yelp_crawler.run()
    # thread_pool.wait()
    # if dump_file:
    #     yelp_crawler.write_csv()
