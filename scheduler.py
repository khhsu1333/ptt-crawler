# coding=utf-8
import gevent.monkey
gevent.monkey.patch_all()

import logging
import os
import sys
import signal
import time
import datetime
import re

import gevent
from gevent.queue import Queue
import requests
requests.packages.urllib3.disable_warnings()

import lib.ptt_parser as parser
import lib.model

error_strings = ['Internal Server Error', 'Service Temporarily Unavailable', '404 - Not Found.']

pendings = Queue()

# 爬蟲參數
DOWNLOADER_SIZE = 5
TIMEOUT = 10.0
WAIT_TIME = 1.0
LONG_DOWNLOAD_TIME = 10.0
ANALYSIS_PERIOD = 600.0


def download(url):
    r = requests.get(url, cookies=dict(over18='1'), verify=False, timeout=TIMEOUT)
    if r:
        return r.text


def downloader(downloader_num):
    global first, done
    first = True

    while not (done and pendings.empty()):
        # 取得要爬的網址
        try:
            metadata = pendings.get(timeout=TIMEOUT)
            if len(metadata[0]) > 1:
                url = metadata[0]
            else:
                url = metadata
        except:
            gevent.sleep(WAIT_TIME)
            continue
        print('\t[{}] {}'.format(downloader_num, url))

        # 下載網頁
        t1 = time.time()
        try:
            html = download(url)
            if not html:
                raise Exception('got empty page')
        except:
            add_requests([metadata])
            continue
        t2 = time.time() - t1
        if t2 > LONG_DOWNLOAD_TIME:
            logging.warning('[{}] long download time ({}): {}'.format(downloader_num, t2, url))

        # 偵測網頁是否含有錯誤訊息
        if any(s in html for s in error_strings):
            logging.debug('server error occurred: {}'.format(url))
            if db.should_retry(url):
                add_requests([metadata])
            else:
                logging.error('[{}] cannot download page: {}'.format(current_board, url))
                done = True if 'index' in url else False
            continue

        spider(url, html, metadata)
        gevent.sleep(WAIT_TIME)

    if current_page < max_page:
        logging.error('[{}] finish crawling before crawling all articles ({}, {})'.format(current_board, max_page, current_page))
    else:
        if first:
            logging.info('[{}] crawling complete. execution time: {}. crawled page number: {}'.format(current_board, time.time()-start_time, page_count))
            first = False


def spider(url, html, metadata):
    err_occured = False
    parsing_time = None
    db_access_time = None

    if 'index' in url:
        # 文章列表頁面

        # 取得文章的網址
        article_tuple_list, is_last_page = parser.get_url_of_articles(html)
        add_requests(article_tuple_list)

        # 檢查是否爬完全部頁面
        page_num = parser.get_page_num(url)
        if is_last_page:
            if page_num < max_page:
                h = db.record_error(current_board, url, html)
                logging.error('[{}] cannot find the url of next page ({}, {}): {}'.format(current_board, max_page, page_num, h))
            global done
            done = True

        # 更新資料庫中爬完的頁面數量
        global current_page
        current_page = page_num
        db.save_current_page_num(current_board, current_page)
    else:
        # 文章頁面

        if metadata is None:
            # 理論上不會發生這種情況
            logging.error('article metadata is empty: {}'.format(url))
            err_occured = True
        else:
            # 分析文章
            try:
                j = parser.parse_article(html, metadata)
                filename = db.record_article(current_board, url, html)

                # 將文章的 metadata 存到資料庫
                article_id = db.insert_article(current_board, j, filename)
                if not article_id:
                    raise Exception('fail to insert the article into the database')
                db.insert_pushs(article_id, j['pushs'])
            except Exception as e:
                print(e)
                logging.debug('{}: {}'.format(e, url))
                db.record_error(current_board, url, html)
                err_occured = True

    # 分析爬蟲效能
    if not err_occured:
        global timer, page_count, prev_page_count
        page_count += 1

        dif = time.time() - timer
        if dif > ANALYSIS_PERIOD:
            rate = (page_count-prev_page_count) / dif
            timer = time.time()
            prev_page_count = page_count
            logging.info('[{}] crawling rate: {} pages/s'.format(current_board, rate))


def add_requests(urls):
    for u in urls:
        pendings.put(u)


def crawl(board, downloader_num):
    # 初始化用來紀錄爬蟲進度的全域變數
    global current_board, current_page, max_page, done
    current_board = board
    done = False

    # 初始化用來分析爬蟲效能的全域變數
    global timer, start_time, page_count, prev_page_count
    timer = time.time()
    start_time = time.time()
    page_count = 0
    prev_page_count = 0

    # 找到討論版目前的頁面數量
    try:
        max_page = parser.get_max_page(current_board)
    except:
        logging.error('[{}] fail to get the max page number'.format(current_board))
        return

    # 建立存放文章頁面的資料夾
    try:
        os.mkdir('data/{}'.format(current_board))
    except:
        pass

    # 從爬蟲上次的進度開始爬網頁
    page_num = db.get_crawled_page(current_board)
    if page_num > max_page:
        logging.error('[{}] exceed max page number ({}, {})'.format(current_board, max_page, page_num))
        return
    elif page_num < max_page:
        current_page = page_num
    else:
        # start to update the articles
        current_page = parser.find_updated_page(page_num)

    # 取得起始頁面的網址
    url = parser.get_board_url(current_board, current_page)
    add_requests([url])

    # 啟動 downloader
    threads = [gevent.spawn(downloader, i+1) for i in xrange(downloader_num)]
    gevent.joinall(threads)
    db.clear_cache()


def graceful_reload(signum, traceback):
    global db
    db.close()


def main():
    # logging 設定
    logging.basicConfig(filename='log/{}.log'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')),
                        format='[%(levelname)s] %(message)s',
                        level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.ERROR)

    # database 設定
    global db
    try:
        db = lib.model.database('ptt', 'ptt')
    except:
        print('cannot connect to the database.')
        return
    signal.signal(signal.SIGHUP, graceful_reload)

    #boards = parser.get_hot_boards()
    boards = ['Movie']
    for b in boards:
        print('crawling board: {}'.format(b))
        crawl(b, downloader_num=DOWNLOADER_SIZE)


if __name__ == '__main__':
    main()
