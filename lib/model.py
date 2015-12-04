# coding=utf-8
import psycopg2
import md5

import lib.ptt_parser as parser

RETRY_TIMES = 5


def get_hash(s):
    return md5.new(s).hexdigest()

class database(object):
    def __init__(self, dbname, user, password=''):
        self.conn = psycopg2.connect(database=dbname, user=user, password=password, host='localhost', port='5432')
        self.cur  = self.conn.cursor()

        self.retry_cache = dict()

    def close(self):
        self.cur.close()
        self.conn.close()

    def clear_cache(self):
        self.retry_cache = dict()

    def get_crawled_page(self, board):
        cmd = "SELECT count from boards WHERE name=%s"
        self.cur.execute(cmd, (board,))
        row = self.cur.fetchone()
        if row:
            num = row[0]
        else:
            num = 1
            cmd = "INSERT INTO boards (name, count) VALUES (%s, %s)"
            self.cur.execute(cmd, (board, 1))
            self.conn.commit()
        return num

    def should_retry(self, url):
        if not url in self.retry_cache:
            self.retry_cache[url] = 0
            return True
        else:
            if self.retry_cache[url] > RETRY_TIMES:
                return False
            else:
                self.retry_cache[url] += 1
                return True

    def save_current_page_num(self, board, page_num):
        cmd = "UPDATE boards SET count=%s WHERE name=%s"
        self.cur.execute(cmd, (page_num, board))
        self.conn.commit()

    def insert_article(self, board, metadata, filename):
        cmd = "INSERT INTO articles (board, author, title, url, filename, content, score) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING article_id"
        self.cur.execute(cmd, (board,
                               metadata['author'],
                               metadata['title'],
                               metadata['url'],
                               filename,
                               metadata['content'],
                               metadata['score']))
        article_id = self.cur.fetchone()[0]
        self.conn.commit()

        return article_id

    def insert_pushs(self, article_id, pushs):
        cmd = "INSERT INTO pushs (name, score, content) VALUES (%s, %s, %s)"
        for p in pushs:
            self.cur.execute(cmd, (p['uid'], p['tag'], p['content']))
        self.conn.commit()

    @staticmethod
    def record_error(board, url, html):
        filename = '{}-{}.html'.format(board, get_hash(url))
        with open('exception/{}'.format(filename), 'wb') as f:
            f.write(html.encode('utf-8'))
        return filename

    @staticmethod
    def record_article(board, url, html):
        h = parser.get_article_hash(url)
        filename = 'data/{}/{}'.format(board, h)
        with open('{}'.format(filename), 'wb') as f:
            f.write(html.encode('utf-8'))
        return filename
