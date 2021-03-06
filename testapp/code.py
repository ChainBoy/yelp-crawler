# -*- coding:utf-8 -*-

# @version: 1.0
# @author: ZhangZhipeng
# @date: 2016-05-12


import os
import hmac
import time
import json
import Queue
import base64
import urllib
import random
import hashlib
import logging
import threading
import subprocess
import requests

downloader = request.session()

from Crypto.Cipher import AES

version_history = ["8.8.0", "8.8.0-BETA2", "8.6.0", "8.5.1", "8.4.1", "8.3.2", "8.2.0", "8.1.1", "7.12.0", "7.10.1", "7.9.0", "7.6.1", "7.5.2", "7.3.1", "7.3.0", "7.2.2", "7.2.1", "7.2.0", "7.1.0", "7.1.0-BETA2", "7.0.3", "7.0.2", "7.0.1", "7.0.0", "7.0.0-BETA5", "6.11.1",
                   "6.11.0", "6.10.1", "6.9.1", "6.8.1", "6.8.0", "6.7.0", "6.6.0", "6.5.2", "6.1.0", "5.12.2", "5.9.0", "5.8.1", "5.7.0", "5.6.1", "5.5.1", "5.4.0", "5.3.2", "5.3.1", "5.2.0", "5.1.0", "5.0.4", "5.0.2", "5.0.1", "5.0.0", "4.4.1", "4.4.0", "4.2.1", "3.9.2", "3.9.0", "3.8.2", "3.8.0"]

char_list = [chr(i) for i in range(97, 123)] + range(10)

net_type_list = ["Verizon", "Sprint", "at&t", "AT&T", "NTT DoCoMo",
                 "T-Mobile", "Vodafone", "Orange", "KDDI", u"中国电信", u"中国移动", u"中国联通"]


class Phone():
    app_version = "3.8.4"
    net_type = u"中国移动"
    device_name = "HWNXT"
    device_id = "HUAWEINXT-AL10"
    android_version = "6.0"
    phone_brand = "HUAWEI"
    android_id = ""
    _default = True

    def _dict(self):
        data = {}
        for key in dir(self):
            if not key.startswith("_"):
                data[key] = getattr(self, key)
        return data


class Device():
    device_name = "WHNXT"
    device_id = "HUAWEI NXT-AL10"
    phone_brand = "Huawei"
    phone_name = "Mate 8"


class DeviceManager():
    with file("devices.json", "rb")as f:
        devices = json.loads(f.read())

    @classmethod
    def new(cls):
        device_json = random.choice(DeviceManager.devices)
        device = Device()
        device.device_name = device_json.get("codename")
        device.device_id = device_json.get("model")
        device.phone_brand = device_json.get("manufacturer")
        device.phone_name = device_json.get("market_name")
        return device


class PhoneManager():

    def new(self):
        phone = Phone()
        for key in dir(phone):
            if not key.startswith("_"):
                method_name = "_" + key
                if hasattr(self, method_name):
                    setattr(phone, key, getattr(self,  method_name)(phone))
                else:
                    logging.info("PhoneManager not find method: %s" %
                                 method_name)
        return phone

    def _app_version(self, phone):
        return random.choice(version_history)

    def _net_type(self, phone):
        return random.choice(net_type_list)

    def _device_name(self, phone):
        if phone._default:
            device = DeviceManager.new()
            phone.device_id = device.device_id
            phone.device_name = device.device_name
            phone.phone_brand = device.phone_brand
            phone.phone_name = device.phone_name
            phone._default = False
            return device.device_name
        else:
            return phone.device_name
        #"HWNXT"

    def _device_id(self, phone):
        "HUAWEI NXT-AL10"
        self._device_name(phone)
        return phone.device_id

    def _android_version(self, phone):
        v1 = [str(i) for i in range(0, 6)]
        v2 = [str(i) for i in range(1, 6)]
        return random.choice(v1) + "." + random.choice(v2)

    def _phone_brand(self, phone):
        "Huawei"
        self._device_name(phone)
        return phone.phone_brand

    def _android_id(self, phone):
        return "".join([str(random.choice(char_list)) for i in range(16)])
        # return "2aa26d0ee4e42b20"


class Task():
    _type = "query_reviews"
    uri = "/reviews"
    # search_shops, search_citys, search_stores
    business_id = "arFjcK63ITZ6RNyGau65xA"
    page_index = 0
    lat = None
    lang = None
    acc = None
    term = None
    location = None

# user_agent = "Version/1 Yelp/v3.8.4 Carrier/%E4%B8%AD%E5%9B%BD%E7%A7%BB%E5%8A%A8 Model/HWNXT OSBuild/HUAWEINXT-AL10 Android/6.0"


# sYwsid = decode("==wdXVzRKlnMzgnYTNXarx2Qv92V5NTW");
# sSecret = decode("==gNOxmWKFUIuVDQ9U3OTZlM8siLqV2QsZkT");

#_sYwsid = decode("==wdk1ybXdVVQhHSD1meyEGTiJlMtFzU");
#_sSecret = decode("==wNjVSKmd2OpcjSTZWMksXVJ1yIFF2IsgGV");

# ywsid = base64.decodestring("==wdXVzRKlnMzgnYTNXarx2Qv92V5NTW"[::-1])
# secret = base64.decodestring("==gNOxmWKFUIuVDQ9U3OTZlM8siLqV2QsZkT"[::-1])
# encrypt_key = "3f2c593b7d469602af5a6fb718bc92cc"


class QueryReviews(threading.Thread):
    ywsid = base64.decodestring("==wdXVzRKlnMzgnYTNXarx2Qv92V5NTW"[::-1])
    secret = base64.decodestring("==gNOxmWKFUIuVDQ9U3OTZlM8siLqV2QsZkT"[
                                 ::-1])  # "NFlCej.+<2VS;u=@5n!AJZlN6"
    encrypt_key = "3f2c593b7d469602af5a6fb718bc92cc"

    def __init__(self, phone_manager, task_queue, flush_device=False):
        super(QueryReviews, self).__init__()
        self._flush_device_flag = flush_device
        self._task_queue = task_queue
        self._phone_manager = phone_manager
        self._Phone = None
        self._obfuscated_params = ["device"]
        self._device_profile_map = {}
        self._query_map = {}
        self._obfuscated_query_map = {}
        self._user_agent = u"Version/1 Yelp/v3.8.4 Carrier/中国移动 Model/HWNXT OSBuild/HUAWEINXT-AL10 Android/6.0"

    def run(self):
        while not self._task_queue.empty():
            task = self._task_queue.get()
            method_name = "_" + task._type
            if not hasattr(self, method_name):
                print "undefined method: %s" % task._type
                continue
            url = getattr(self, method_name)(task)
            # self._query_reviews(task)
            self._task_queue.task_done()



    def _query_reviews(self, task):
        self._build_device(flush=self._flush_device_flag)

        self._build_query("business_id", task.business_id)
        self._build_query("offset", task.page_index * 50)
        self._build_query("limit", 50)
        self._build_obfuscated("location_lat", task.lat)
        self._build_obfuscated("location_long", task.lang)
        self._build_obfuscated("location_acc", task.acc)

        url = self._build_url()
        print "------------- search reviews -------------"
        print url
        print "=========================================="
        return url

    def _search_shops(self, task):
        "搜索商品名"
        self._build_device(flush=self._flush_device_flag)

        self._build_query("term", task.term)
        url = self._build_url(task.uri)
        print "------------- search  shops -------------"
        print url
        print "========================================="
    def _search_citys(self, task):
        "搜索城市"
        "搜索商品名"
        self._build_device(flush=self._flush_device_flag)

        self._build_query("location", task.location)
        url = self._build_url(task.uri)
        print "------------- search  citys -------------"
        print url
        print "========================================="
        return url

    def _search_stores(self, task):
        "搜索商店列表"
        self._build_device(flush=self._flush_device_flag)

        self._build_query("term" ,task.term )
        self._build_query("location" ,task.location )
        self._build_query("limit" ,task.limit )
        self._build_query("offset", task.offset)
        self._build_query("mode" ,task.mode )
        self._build_query("fmode", task.fmode)

        url = self._build_url(task.uri)
        print "------------- search stores -------------"
        print url
        print "========================================="
        return url

    def _build_url(self, uri="/reviews"):
        string = ""
        for k, v in self._query_map.items():
            string += "%s=%s&" % (k, urllib.quote(str(v)))

        for k, v in self._device_profile_map.items():
            if k in self._query_map.keys() or k in self._obfuscated_params:
                continue
            string += "%s=%s&" % (k, urllib.quote(str(v)))

        efs = self.entry_obfuscate(self._device_profile_map)
        print "[-- efs] == ", efs
        string += "efs=" + urllib.quote(efs) + "&"

        sign = self.generate_sign(uri)
        string += "signature=" + urllib.quote(sign)
        # string += "signature=" + urllib.quote(sign)#.replace("/", "%2F")
        return "http://auto-api.yelp.com" + uri + "?" + string

    def _build_device(self, Phone=None, flush=False):
        if not Phone:
            Phone = self._Phone or self._phone_manager.new()
            if flush:
                Phone = self._phone_manager.new()
        self._device_profile_map = {
            "ywsid": QueryReviews.ywsid,
            "device": Phone.android_id,
            "device_type": (Phone.phone_brand + "+" + Phone.device_name + "/" + Phone.device_id),
            "app_version": Phone.app_version,
            "cc": "US",
            "lang": "en"
        }
        # print Phone._dict()
        self._user_agent = "Version/1 Yelp/v%(app_version)s Carrier/%(net_type)s Model/%(device_name)s OSBuild/%(device_id)s Android/%(android_version)s" % Phone._dict()

    def _build_query(self, key, value):
        if not self._query_map:
            self._query_map = {
                "lang": "en",
                # "xref": "",
                "time": int(time.time()),
                "nonce": base64.b64encode(os.urandom(4)),
            }
        self._query_map[key] = value

    def _build_obfuscated(self, key, value):
        if not self._obfuscated_query_map:
            self._obfuscated_query_map = {
                # "location_lat": "29.6183721",
                # "location_long": "-95.6387007",
                # "location_acc": "8",
                # "latitude": "29.6183721",
                # "longitude": "-95.6387007"
            }
        self._obfuscated_query_map[key] = value

    def entry_obfuscate(self, map_1):
        map_2 = dict(self._obfuscated_query_map)
        for k, v in map_1.items():
            if k in self._obfuscated_params:
                map_2[k] = v
        string = ""
        for k, v in map_2.items():
            string += "%s=%s&" % (k, urllib.quote(str(v)))
        # key = str(int(self.encrypt_key, 16))
        # TODO: 这里需要注意, 跟java实现方式不太相似，可能不正确
        string = string[:-1] if string and string[-1] else string
        print "entry string: ", string
        # string = "device=2aa26d0ee4e42b20&latitude=NaN&longitude=NaN"
        # string = "device=2aa26d0ee4e42b20&location_acc=35.0&location_long=116.319634&location_lat=40.08767"
        cmd = 'java Code "%s"' % string
        sub = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        sub.wait()
        out_str = sub.stdout.read().strip()
        if string in out_str and "[OK]" in out_str:
            data = out_str.split("\n")[-1].split(":")[-1].strip()[1:-1]
        else:
            data = ""
        return data

        # key = self.encrypt_key
        # iv = "\x00" * 16
        # BS = AES.block_size
        # pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
        # print "SSSSSSTR=== ", string
        # print "PPPPPPAD=== ", pad(string)
        # cipher = AES.new(key, AES.MODE_CBC, IV=iv)
        # # data = base64.encodestring((cipher.encrypt(string))).rstrip()
        # data = base64.encodestring((cipher.encrypt(pad(string)))).rstrip()
        return data

        # mCipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
        # localSecretKeySpec = new SecretKeySpec(key, "AES");
        # localIvParameterSpec = new IvParameterSpec(new byte[16]);
        # mCipher.init(1, localSecretKeySpec, localIvParameterSpec);
        # String.valueOf(Base64Coder.encode(mCipher.doFinal(str1.getBytes())));

    def generate_sign(self, uri="/reviews"):
        map_1 = dict(self._device_profile_map)
        map_1.update(self._query_map)
        map_1["efs"] = self.entry_obfuscate(map_1)
        for k in self._obfuscated_params:
            try:
                del map_1[k]
            except:
                pass
        string = uri
        params_list = []
        for k, v in map_1.items():
            params_list.append("%s=%s" % (k, str(v)))
        params_list.sort()
        string += "".join(params_list)
        # string = "123"

        # Mac localMac = Mac.getInstance("HmacSHA1");
        # localMac.init(new SecretKeySpec(query_string.getBytes(), "HmacSHA1"));
        # "_" + String.valueOf(Base64Coder.encode(localMac.doFinal(secret.getBytes())));
        print "sign text: ", string
        data = hmac.new(self.secret, string, hashlib.sha1).digest().encode(
            'base64').rstrip()
        return "_" + data

# "location_lat": "29.6183721",
# "location_long": "-95.6387007",
# "location_acc": "8",


def test():
    task_queue = Queue.Queue()
    for i in range(5):
        task = Task()
        task.business_id = "vaMGN4lUJn4zOXkn2icIIQ"
        task.page_index = i
        task._type = "query_reviews"
        task.lat = "29.6183721"
        task.lang = "-95.6387007"
        task.acc = "8"

        task_queue.put(task)
    phone_manager = PhoneManager()
    threads = []
    for i in range(1):
        threads.append(QueryReviews(
            phone_manager, task_queue, flush_device=False))
    for i in threads:
        i.start()
    for i in threads:
        i.join()

def test_search_shops():
    print "test search shop: [kfc] ..."
    phone_manager = PhoneManager()
    task_queue = Queue.Queue()

    task = Task()
    task.term = "kfc"
    task._type = "search_shops"
    task.uri = "/suggest"

    task_queue.put(task)
    QueryReviews(phone_manager, task_queue, flush_device=False).start()

def test_search_citys():
    print "test search city: [ka] ..."
    phone_manager = PhoneManager()
    task_queue = Queue.Queue()

    task = Task()
    task.location = "ka"
    task._type = "search_citys"
    task.uri = "/suggest/locations"

    task_queue.put(task)
    QueryReviews(phone_manager, task_queue, flush_device=False).start()

def test_search_stores():
    print "test search stores: [Katy, TX][kfc drive thru] ..."
    phone_manager = PhoneManager()
    task_queue = Queue.Queue()

    task = Task()
    task.term = "kfc drive thru"
    task.location = "Katy, TX"
    task.limit = 20
    task.offset= 0
    task.mode = 1
    task.fmode=0


    task._type = "search_stores"
    task.uri = "/search"

    task_queue.put(task)
    QueryReviews(phone_manager, task_queue, flush_device=False).start()

if __name__ == '__main__':
    # test_search_shops()
    # test_search_citys()
    # test_search_stores()
    pass
