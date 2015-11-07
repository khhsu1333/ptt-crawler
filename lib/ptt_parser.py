# coding=utf-8
import re
import json
import logging

import requests
from pyquery import PyQuery as pq


host_url = 'https://www.ptt.cc'
board_url = 'https://www.ptt.cc/bbs/{}/index{}.html'

re_page_num = re.compile(r'index(\d+).html')
re_ip = re.compile(r'\d+\.\d+\.\d+\.\d+')
re_board = re.compile(r'/bbs/(.+)/index\.html')


def get_max_page(board):
    re_page_num = re.compile(r'index(\d+).html')

    html = requests.get(get_board_url(board), cookies=dict(over18='1')).text
    d = pq(html)
    link = d('.pull-right a').eq(1).attr('href')
    return int(re_page_num.search(link).group(1))+1


def get_board_url(board, index=''):
    return board_url.format(board, index)


def get_page_num(url):
    return int(re_page_num.search(url).group(1))


def get_article_hash(url):
    pos = url.rfind('/')
    return url[pos+1:]


def get_hot_boards():
    hot_boards_url = 'https://www.ptt.cc/hotboard.html'
    re_hot_board = re.compile(r'<td width="120"><a href="/bbs/(.+)/index\.html">')

    html = requests.get(hot_boards_url).text
    boards = []
    for link in re_hot_board.finditer(html):
        b = link.group(1)
        if not b in boards:
            boards.append(b)
    return boards



def get_article_url_list(raw_html):
    urls = []
    is_last = False

    d = pq(raw_html)
    for link in d('.title a').map(lambda: host_url + pq(this).attr('href')):
        urls.append(link)

    next_page = d('.pull-right a').eq(2).attr('href')
    if next_page:
        urls.append(host_url + next_page)
    else:
        is_last = True

    return (urls, is_last)


def get_article_json(raw_html):
    d = pq(raw_html)

    items = []
    for c in d('.article-meta-value'):
        items.append(c.text)

    if len(items) == 4:
        author = items[0]
        title = items[2]
        date = items[3]
    elif len(items) == 3:
        author = items[0]
        title = items[1]
        date = items[2]
    else:
        raise Exception('fail to parse article header, length: {}'.format(len(items)))

    m = re_ip.search(d.text())
    if m:
        ip = m.group()
    else:
        raise Exception('fail to find author ip')

    #a = d('#main-container').html()
    #a = a.split("</div>")
    #a = a[4].split('<span class="f2">※ 發信站: 批踢踢實業坊(ptt.cc),')
    #content = a[0]

    content = d.text()

    j = dict(aid=1, author=author, title=title, date=date, ip=ip, content=content)
    return json.dumps(j)
