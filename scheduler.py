# coding=utf-8
import logging
import os
import sys
import signal
import time
import datetime
import re

import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent.queue import Queue
import requests
requests.packages.urllib3.disable_warnings()

import lib.ptt_parser as parser
import lib.model


db = lib.model.database()

pendings = Queue()

error_strings = [b'Internal Server Error', b'Service Temporarily Unavailable']

TIMEOUT = 10.0
WAIT_TIME = 1.0
LONG_DOWNLOAD_TIME = 10.0
ANALYTICS_PERIOD = 60.0


def download(url):
    r = requests.get(url, cookies=dict(over18='1'), verify=False, timeout=TIMEOUT)
    if r:
        return r.text


def downloader(downloader_num):
    global finish_first
    finish_first = True

    while not (done and pending.empty()):
        # get url from pending queue
        try:
            url = pendings.get(timeout=TIMEOUT)
        except:
            gevent.sleep(WAIT_TIME)
            continue
        print('\t({}) {}'.format(downloader_num, url))

        # download page
        t1 = time.time()
        try:
            html = download(url)
        except Exception as e:
            logging.debug('({}) download error at: {}'.format(downloader_num, url))
            add_requests([url])
            continue
        t2 = time.time() - t1
        if t2 > LONG_DOWNLOAD_TIME:
            logging.warning('({}) long download time at ({}): {}'.format(downloader_num, t2, url))

        # error detection
        if any(s in html for s in error_strings):
            logging.debug('server error occurred at: {}'.format(url))
            if 'index' in url:
                # article list page
                if parser.get_page_num(url) <= max_page:
                    add_requests([url])
            else:
                # article page
                if db.retry_ok(url):
                    add_requests([url])
                else:
                    logging.error('[{}] cannot download page: {}'.format(crawled_board, url))
                    db.recording_error_page(crawled_board, parser.get_article_hash(url), html, error_type='download')
            continue

        spider(url, html)
        gevent.sleep(WAIT_TIME)

    if crawled_page_num < max_page:
        logging.error('[{}] finish crawling before done, max={}, crawled={}'.format(crawled_board, max_page, crawled_page_num))
    else:
        if finish_first:
            logging.info('[{}] crawling complete. execution time: {}. page size: {}'.format(crawled_board, time.time()-start_time, page_count))
            finish_first = False


def spider(url, html):
    is_err = False
    if 'index' in url:
        # article list page
        page_num = parser.get_page_num(url)
        links, is_last = parser.get_article_url_list(html)

        # filter repeat url
        t1 = time.time()
        urls = db.filter_repeat_pages(links)
        add_requests(urls)
        logging.info('[{}] db access time: {}'.format(crawled_board, time.time() - t1))

        # check if the crawling is complete
        if is_last:
            global done
            if page_num < max_page:
                if db.retry_ok(url):
                    add_requests([url])
                else:
                    logging.error('[{}] cannot find next page url. max={}. current={}'.format(crawled_board, max_page, page_num))
                    db.recording_error_page(crawled_board, '{}.html'.format(page_num), html, error_type='download')
                    done = True
            else:
                done = True

        # record crawled page number
        global crawled_page_num
        if crawled_page_num >= page_num:
            logging.error('repeated crawling index page at: {}'.format(url))
        crawled_page_num = page_num
        db.set_crawled_page_num(crawled_board, page_num)
    else:
        # article page
        try:
            t1 = time.time()
            j = parser.get_article_json(html)
            db.store_article(crawled_board, parser.get_article_hash(url), j)
        except Exception as e:
            logging.debug('parsing error at ({}): {}'.format(e, url))
            db.recording_error_page(crawled_board, parser.get_article_hash(url), html, error_type='parsing')
            is_err = True
            db.record_parsing_error(url)

    # analytics
    if not is_err:
        global timer, page_count, previous_page_count
        page_count += 1

        diff = time.time() - timer
        if diff > ANALYTICS_PERIOD:
            rate = (page_count-previous_page_count)/diff
            timer = time.time()
            previous_page_count = page_count
            logging.info('[{}] crawling rate: {} pages/second'.format(crawled_board, rate))


def pipeline(name, content):
    with open('data/{}/{}'.format(crawled_board, name), 'w') as f:
        f.write(content)


def error_logging(name, content):
    with open('exception/{}-{}'.format(crawled_board, name), 'wb') as f:
        f.write(content)


def add_requests(urls):
    for u in urls:
        pendings.put(u)


def crawl(board, downloader_num):
    # init
    global crawled_board, start_time, max_page, crawled_page_num, done
    crawled_board = board
    start_time = time.time()
    done = False

    # variable for analytics
    global timer, page_count, previous_page_count
    timer = time.time()
    page_count = 0
    previous_page_count = 0

    # find max page number
    try:
        max_page = parser.get_max_page(board)
    except:
        logging.error('fail to get max page of board: {}'.format(board))
        return

    # create a folder to store the articles
    try:
        os.mkdir('data/{}'.format(board))
    except:
        pass

    # start from the page stopped last time
    crawled_page_num = db.get_crawled_page_num(board)
    if crawled_page_num > max_page:
        logging.error('[{}] exceed max page, max={}, crawled={}'.format(board, max_page, crawled_page_num))
        return
    url = parser.get_board_url(board, crawled_page_num)

    add_requests([url])
    threads = [gevent.spawn(downloader, i+1) for i in xrange(downloader_num)]
    gevent.joinall(threads)


def graceful_reload(signum, traceback):
    # Explicitly close some global MongoClient object
    db.close()


def main():
    d = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    logging.basicConfig(filename='log/{}.log'.format(d), format='[%(levelname)s] %(message)s', level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.ERROR)
    signal.signal(signal.SIGHUP, graceful_reload)

    boards = parser.get_hot_boards()
    for b in boards:
        print('crawling board: {}'.format(b))
        crawl(b, downloader_num=8)


if __name__ == '__main__':
    main()
