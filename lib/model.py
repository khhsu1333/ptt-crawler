from pymongo import MongoClient


RETRY_TIMES = 5


class database(object):
    def __init__(self):
        self.client = MongoClient()
        db = self.client.ptt_crawler
        self.ptt_boards = db.boards
        self.ptt_articles = db.articles

    def close(self):
        self.client.close()

    def filter_repeat_pages(self, links):
        urls = []
        for link in links:
            key = link.replace('.', '-')
            if 'index' in link or (self.ptt_articles.find_one({key: 'crawled'}) is None):
                self.ptt_articles.insert({key: 'crawled'})
                urls.append(link)
        return urls

    def get_crawled_page_num(self, board):
        b = self.ptt_boards.find_one({'board': board})
        if b:
            page_num = int(b['count'])
        else:
            page_num = 1
            self.ptt_boards.insert({'board': board, 'count': 1})
        return page_num

    def set_crawled_page_num(self, board, page_num):
        self.ptt_boards.update_one({'board': board}, {'$set': {'count': page_num}})

    def retry_ok(self, url):
        key = url.replace('.', '-')
        m = self.ptt_articles.find_one({'error_url': key})
        if m is None:
            self.ptt_articles.insert({'error_url': key, 'count': 0})
            return True
        else:
            if m['count'] > RETRY_TIMES:
                return False
            else:
                self.ptt_articles.update_one({'error_url': key}, { '$inc': {'count': 1}})
                return True

    def record_parsing_error(self, url):
        key = url.replace('.', '-')
        self.ptt_articles.insert({'error_url': key, 'parse_error': True})

    @staticmethod
    def store_article(board, name, content):
        with open('data/{}/{}'.format(board, name), 'w') as f:
            f.write(content)

    @staticmethod
    def recording_error_page(board, name, content, error_type):
        with open('exception/{}-{}-{}'.format(board, error_type, name), 'w') as f:
            f.write(content)
