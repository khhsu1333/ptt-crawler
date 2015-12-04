# coding=utf-8
import re
import logging

import psycopg2
import jieba

re_valid_word = re.compile(r'^[\u4e00-\u9fa5_a-z0-9]+$')

jieba.set_dictionary('data/dict.txt.big')


dbname = 'ptt'
user = 'ptt'
password = '1234'


def get_words(content):
    words = jieba.cut(content, cut_all=False)
    return words


if __name__ == '__main__':
    print('安安'.lower())
    logging.getLogger("jieba").setLevel(logging.ERROR)

    conn = psycopg2.connect(database=dbname, user=user, password=password, host='localhost', port='5432')
    cur  = conn.cursor()

    while True:
        cur.execute('SELECT * from articles WHERE processed=false LIMIT 1')
        row = cur.fetchone()
        if not row:
            print('Building complete')
            break

        # article: id, board, author, title, url, filename, length, content, score, processed
        article_id = row[0]
        board = row[1]
        title = row[3]
        content = row[7]
        score = row[8]

        words = dict()
        count = 0

        words_iter = get_words(content)
        for w in words_iter:
            m = re_valid_word.search(w)
            if m:
                word = m.group(0).lower()
                if word in words:
                    words[word] += 1
                else:
                    words[word] = 1
                count += 1

        if count == 0:
            print('empty article: {}'.format(title))

        # build indexing
        cmd = 'INSERT INTO words (word, article_id, count) VALUES (%s, %s, %s)'
        for k, v in words.items():
            cur.execute(cmd, (k, article_id, v))

        # update article information
        cmd = "UPDATE articles SET length=%s, processed=%s WHERE article_id=%s"
        cur.execute(cmd, (count, True, article_id))

        conn.commit()
        print('complete: {}'.format(title))

    cur.close()
    conn.close()
