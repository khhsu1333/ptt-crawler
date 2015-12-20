# coding=utf-8
import re
import logging

import requests
from pyquery import PyQuery as pq


host_url = 'https://www.ptt.cc'

re_page_num = re.compile(r'index(\d+).html')
re_ip = re.compile(r'\d+\.\d+\.\d+\.\d+')
re_board = re.compile(r'/bbs/(.+)/index\.html')


def get_max_page(board):
    re_page_num = re.compile(r'index(\d+).html')

    html = requests.get(get_board_url(board), cookies=dict(over18='1')).text
    d = pq(html)
    link = d('.pull-right a').eq(1).attr('href')
    return int(re_page_num.search(link).group(1))+1


def find_updated_page(page_num):
    # TODO: use hashed content to find the updated page
    return page_num


def get_board_url(board, index=''):
    board_url = 'https://www.ptt.cc/bbs/{}/index{}.html'
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


def get_url_of_articles(html):
    urls = []
    is_last_page = False

    d = pq(html)
    for block in d('.r-ent'):
        b = pq(block)
        # 標題
        title = b('.title').text()
        article_url = b('.title a').attr('href')
        if not article_url:
            print('cannot get url: {}'.format(title))
            continue
        url = host_url + article_url
        # 日期
        date = b('.date').text()
        # 作者
        author = b('.author').text()
        urls.append((url, title, author, date))

    next_page = d('.pull-right a').eq(2).attr('href')
    if next_page:
        urls.append(host_url + next_page)
    else:
        is_last_page = True

    return (urls, is_last_page)


def parse_article(html, metadata):
    d = pq(html)

    content = d('#main-content').clone().children().remove().end().text()

    pushs = d('div.push').map(lambda:{
            'tag': pq(this)('.push-tag').text(),
            'uid': pq(this)('.push-userid').text(),
            'content': pq(this)('.push-content').text().replace(': ', '')
        })

    score = 0
    for p in pushs:
        if p['tag'] == u'推':
            score += 1
            p['tag'] = 1
        elif p['tag'] == u'噓':
            score -= 1
            p['tag'] = -1
        else:
            p['tag'] = 0

    return dict(url=metadata[0],
                title=metadata[1],
                author=metadata[2],
                date=metadata[3],
                content=content,
                pushs=pushs,
                score=score)

if __name__ == '__main__':
    url = 'https://www.ptt.cc/bbs/Japan_Travel/index1204.html'
    html = requests.get(url, cookies=dict(over18='1')).text
    urls, is_last = get_article_urls(html)
    for u in urls:
        pass
