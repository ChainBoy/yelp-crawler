# -*- coding:utf-8 -*-

# @version: 1.0
# @author: ZhangZhipeng
# @date: 2016-03-10

import re
import time
import random
import logging

import requests


class ProxySetting:
    encoding = "gbk"
    proxy_server = "http://127.0.0.1:26680/proxy"
    thread_proxy_pool_min_size = 10
    proxy_sleep_time = 5
    proxy_black_sleep_time = 240
    check_proxy = True
    check_proxy_url = "http://www.yelp.com/hovercard/business?id=Dj8wUyvU2Y4Ejg7oXOo9YA"
    proxy_good_mark = "Charlotte"


class ProxyManager(object):
    _instance = None
    _index = 0

    def __init__(self, proxy_setting):
        self._proxy_setting = proxy_setting
        self._proxy_all = set([])
        self._proxy_using = set([])
        self._black_proxy_manager = BlackProxyManager()

    @classmethod
    def get_instance(cls, proxy_setting=None):
        if not cls._instance:
            cls._instance = ProxyManager(proxy_setting)
        return cls._instance

    def update_proxy_pool(self):
        proxy_list = self._get_proxy_source()
        print "get proxy from server count: %s, normal: waiting..." % len(proxy_list)
        normal_count = 0
        for proxy in proxy_list:
            if self._check_proxy_connect(proxy):
                self._proxy_all.add(proxy)
                self._proxy_using.add(proxy)
                normal_count += 1
        print "get proxy from server count: %s, normal: %s" % (len(proxy_list), normal_count)

    def get(self, sleep_time=5):
        update_num = 0
        try_num = 0
        while True:
            if len(self._proxy_using) < self._proxy_setting.thread_proxy_pool_min_size:
                print "get new proxy from server"
                # 如果可用代理不足n条，获取新代理
                self.update_proxy_pool()
                update_num += 1
                if update_num > 10:
                    try:
                        proxy = random.choice(list(self._proxy_using))
                        self.remove(
                            proxy, sleep_time=sleep_time or self._proxy_setting.proxy_sleep_time)
                        return proxy
                    except IndexError:
                        print "why index error?"
                        return None
                time.sleep(2)
                continue
            if try_num >= len(self._proxy_all):
                # 如果尝试次数超过代理数量，随即返回一个可用代理
                proxy = random.choice(self._proxy_using)
                self.remove(
                    proxy, sleep_time=sleep_time or self._proxy_setting.proxy_sleep_time)
                return proxy
            try:
                proxy = list(self._proxy_all)[self._index]
            except IndexError:
                # 如果索引超限，设置index为0 重新开始
                self._index = 0
                proxy = list(self._proxy_all)[0]
            self._index += 1
            try_num += 1

            if self._black_proxy_manager.has(proxy):
                try:
                    self._proxy_using.remove(proxy)
                except:
                    pass
                continue
            self._proxy_using.add(proxy)
            self.remove(
                proxy, sleep_time=self._proxy_setting.proxy_sleep_time)
            return proxy

    def remove(self, proxy, sleep_time=300, new=False):
        self._black_proxy_manager.add(proxy, sleep_time=sleep_time or self._proxy_setting.proxy_black_sleep_time)
        if new:
            return self.get()

    def _get_proxy_source(self, count=500):
        url = self._proxy_setting.proxy_server + "?count=%s" % count
        print "proxy server:" + url
        proxy_list = []
        try:
            proxy_list_tmp = requests.get(url).content.split("\n")
            for i in proxy_list_tmp:
                try:
                    proxy_list.append(i.split(",")[0])
                except IndexError:
                    pass
        except Exception, e:
            print "Error", e
            logging.debug("get proxy from server:%s error:%s" % (url, e))
        return proxy_list

    def _check_proxy_connect(self, proxy):
        if not self._proxy_setting.check_proxy:
            return True
        try:
            res = requests.get(self._proxy_setting.check_proxy_url, timeout=20)
            html = res.content
            if re.findall(self._proxy_setting.proxy_good_mark, html):
                print "[OK] check proxy: %s" % proxy
                return True
            else:
                print "[NO] check proxy: %s, code: %s" % (proxy, res.status_code)
                return False
        except Exception, e:
            print "[Error] check proxy: %s, error: %s" % (proxy, e)
            return False


class BlackProxyManager(object):

    def __init__(self):
        self._proxy = {}

    def add(self, proxy, sleep_time=60):
        # 暂停x秒
        self._proxy[proxy] = time.time() + sleep_time

    def has(self, proxy):
        # 判断proxy是否在黑名单中，如果在黑名单中，检查时间，如果可以释放，则返回false；否则返回true，无法使用
        if self._proxy.get(proxy):
            return not self._check_release(proxy)
        else:
            return False

    def _check_release(self, proxy):
        now = time.time()
        release_time = self._proxy.get(proxy, now + 60)
        if now >= release_time:
            # 如果大于预定的释放时间，就将其删除
            del self._proxy[proxy]
            return True
        else:
            return False


def get_proxy_manager(Setting=None):
    if not Setting:
        Setting = ProxySetting
    return ProxyManager.get_instance(Setting)



if __name__ == '__main__':
    class ProxySetting:
        encoding = "gbk"
        proxy_server = "http://127.0.0.1:26680/proxy"
        thread_proxy_pool_min_size = 20
        proxy_sleep_time = 5
        proxy_black_sleep_time = 240
        check_proxy = False
        check_proxy_url = "http://club.jd.com/productpage/p-1034990345-s-0-t-1-p-200.html"
        proxy_good_mark = "UserLevelName"

    proxy_manager = ProxyManager.get_instance(ProxySetting)
    print proxy_manager.get()
