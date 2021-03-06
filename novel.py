# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import

# 追书神器api
import asyncio
import aiohttp
from aiohttp import ClientSession
import async_timeout

import codecs

import os
from six.moves.urllib.parse import urljoin, quote
import logging
from logging.handlers import RotatingFileHandler

import requests
from bson import ObjectId
import motor.motor_asyncio

from config import BASE_DOWNLOAD_DIR

logging.basicConfig(level=logging.DEBUG)

# PYTHONASYNCIODEBUG=1
logging.getLogger('asyncio').setLevel(logging.ERROR)
# logger = logging.getLogger('asyncio')
# logger.handlers = []
logger = logging.getLogger(__file__)

formatter = logging.Formatter(
    '[%(levelname)s]'
    '  %(asctime)s %(levelname)s: %(message)s '
)
detail_formatter = logging.Formatter(
    '[%(levelname)s][%(pathname)s:%(lineno)d][%(funcName)s]'
    '  %(asctime)s %(levelname)s: %(message)s '
)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
file_handler = RotatingFileHandler('novel.log', maxBytes=1024*1024, backupCount=2)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(detail_formatter)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


class BookApi(object):
    def __init__(self, base_url):
        self.base_url = base_url

    def api_get(self, path, params=None, data=None):
        r = requests.get(urljoin(self.base_url, path), params=params, data=data)
        r.raise_for_status()
        return r.json()

    def get_cats(self):
        return self.api_get('/cats/lv2/statistics')

    def get_sub_cats(self):
        return self.api_get('/cats/lv2/')

    def books_by_cat(self, **kwargs):
        '''
        gender: 男生:male 女生:female 出版:press
        type: 热门:hot 新书:new 好评:repulation 完结: over 包月: month
        major: 大类别 从接口1获取
        minor: 小类别 从接口4获取 (非必填)
        start: 分页开始页
        limit: 分页条数
        '''
        return self.api_get('/book/by-categories', params=kwargs)

    def book_info(self, id):
        return self.api_get('/book/'+id)

    def book_source(self, id, legal=False):
        if legal:
            path = '/btoc?view=summary&book='+id
        else:
            path = '/atoc?view=summary&book='+id

        return self.api_get(path)

    def book_chapters(self, id, legal=False):
        return self.api_get('/mix-atoc/%s?view=chapters' % id)

    def chapter_content(self, chapter_link):
        return self.api_get('chapter/' + quote(chapter_link, safe=':'))



async def api_get(url, params=None, data=None):
    async with sem:
        async with ClientSession() as session:
            async with session.get(url, params=params, data=data) as r:
                # logger.debug('Request Url: %s' % urljoin(url,  quote_plus(params)))
                logger.debug('Request Url: %s' % r.url)
                
                if r.status != 200:
                    try:
                        result = await r.json()
                        if '你正在使用的版本已不再提供支持' in result['body']:
                            logger.error('Request %s failed, API not support' % url)
                        else:
                            logger.error('Request %s failed, detail: %s' % (url, result))
                    except Exception as e:
                        logger.error('Request %s failed' % url)

                    return None
                # 这里拿到session 还没有开始读内容，读内容又是一个ansync 操作

                result = await r.json()
                if result['ok'] == False:
                    logger.error('Request api false , url: %s , result:%s ' % (url, result) )
                    return None

                return result


from requests.compat import urljoin, quote_plus

class AsyncBookApi(object):
    def __init__(self, base_url, chapter_url):
        self.base_url = base_url
        self.chapter_url = chapter_url

    async def get_cats(self):
        return await api_get(self.base_url + '/cats/lv2/statistics')

    async def get_sub_cats(self):
        return await api_get(self.base_url + '/cats/lv2/')

    async def books_by_cat(self, **kwargs):
        '''
        gender: 男生:male 女生:female 出版:press
        type: 热门:hot 新书:new 好评:repulation 完结: over 包月: month
        major: 大类别 从接口1获取
        minor: 小类别 从接口4获取 (非必填)
        start: 分页开始页
        limit: 分页条数

        注意，参数顺序有关
        '''



        # must pass  gender, major

        return await api_get(self.base_url + '/book/by-categories?gender={gender}&type={type}&major={major}&start={start}&limit={limit}'.format(**kwargs))

    async def book_info(self, id):
        return await api_get(self.base_url + '/book/'+id)

    async def book_source(self, id, legal=False):
        if legal:
            path = '/btoc?view=summary&book='+id
        else:
            path = '/atoc?view=summary&book='+id

        return await api_get(self.base_url + path)

    async def book_chapters(self, id, legal=False):
        resp = await api_get(self.base_url + '/mix-atoc/%s?view=chapters' % id)
        if resp:
            resp = unify_resp_chapters(resp)
        return resp

    async def chapter_content(self, chapter_link):
        resp = await api_get(self.chapter_url + '/chapter/' + quote(chapter_link, safe=':'))
        if resp:
            resp = unify_resp_chapter(resp)
            resp['link'] = chapter_link
        return resp



async def fetch(url, session):
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        return await response.read()


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(url, session)


def ensure_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def rename_floder(dirname):
    return dirname.replace('/', '／').replace('?', '？').replace(':', '：')\
            .replace('*', '＊').replace('"', ' ').replace('|', ' ')


def parse_chapter(resp_chapter):
    if 'title' in resp_chapter:
        title = resp_chapter['title']
        content = resp_chapter['cpContent']
        logger.info('resp_chapter ::::: %s ' % resp_chapter)
        raise Exception('EXIT')
    elif 'chapter' in resp_chapter:
        title = resp_chapter['chapter']['title']
        content = resp_chapter['chapter']['body']
    else:
        raise Exception('Not see', resp_chapter)

    return title, content

def unify_resp_chapter(resp_chapter):
    if 'title' in resp_chapter:
        resp_chapter['content'] = resp_chapter['cpContent']
        del resp_chapter['cpContent']
        # title = resp_chapter['title']
        # content = resp_chapter['cpContent']
        # logger.info('resp_chapter ::::: %s ' % resp_chapter)
        # raise Exception('EXIT')
    elif 'chapter' in resp_chapter:
        resp_chapter = resp_chapter['chapter']
        resp_chapter['content'] = resp_chapter['body']
        del resp_chapter['body']
    else:
        raise Exception('Not see', resp_chapter)

    return resp_chapter


def unify_resp_chapters(resp_chapters):
    if 'chapters' in resp_chapters:
        c_info = resp_chapters
        if 'ok' in c_info:
            del c_info['ok'] 
    elif 'mixToc' in resp_chapters:
        c_info = resp_chapters['mixToc']
    else:
        raise Exception('error format %s' % resp_chapters)

    return c_info



async def download_book_separately(api, book):
    '''
    download book's chapters in separated book folder, with different txt files
    '''
    book_id, book_title = book['_id'], rename_floder(book['title'])

    logger.info('Download book: %s(%s)' % (book_title, book_id) )

    download_dir = os.path.join(BASE_DOWNLOAD_DIR, book['major'], book_title)

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    else:
        logger.info('Skip %s' % book_title)
        return

    resp_chapters = await api.book_chapters(book_id)
    if not resp_chapters:
        logger.error('Can not download %s' % book_title)
        return

    resp_chapters = unify_resp_chapters(resp_chapters)
    chapters = resp_chapters['chapters']

    await insert_not_exist('bookchapters', resp_chapters)


    tasks = []
    for idx, chap in enumerate(chapters):
        resp_chapter = await api.chapter_content(chap['link'])
        if resp_chapter:
            title, content = parse_chapter(resp_chapter)
            
            resp_chapter['link'] = chap['link']
            await db['chapter'].insert_one({'link': chap['link']}, resp_chapter, upsert=True)

            path = os.path.join(download_dir, '{0:0>5d}.txt'.format(idx+1))
            with codecs.open(path, 'w' , 'utf-8') as f:
                f.write(content)


async def download_book(api, book, download_each_chapter=True):
    book_id, book_title = book['_id'], rename_floder(book['title'])

    logger.info('Download book: %s(%s)' % (book_title, book_id) )

    resp_chapters = await api.book_chapters(book_id)
    if not resp_chapters:
        logger.error('Can not download %s' % book_title)
        return False

    await upsert('bookchapters', resp_chapters)

    if download_each_chapter:

        tasks = []
        for idx, chap in enumerate(resp_chapters['chapters']):
            # resp_chapter = await api.chapter_content(chap['link'])
            task = asyncio.ensure_future(api.chapter_content(chap['link']))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)

        chapter_texts =[]
        for resp_chapter in responses:
            if resp_chapter:
                await db['chapter'].update_one({'link': resp_chapter['link']},  {'$setOnInsert': resp_chapter}, upsert=True)

                chapter_texts.append(resp_chapter['content'])

        path = os.path.join(BASE_DOWNLOAD_DIR, '{0}.txt'.format(book_title))

        with codecs.open(path, 'w' , 'utf-8') as f:
            f.write('\n\n'.join(chapter_texts))

    return True


async def insert_not_exist(col_name, obj):
    _id = ObjectId(obj['_id'])
    del obj['_id']
    await db[col_name].insert_one({"_id": _id}, {'$setOnInsert': obj}, upsert=True)


async def upsert(col_name, obj):
    _id = ObjectId(obj['_id'])
    del obj['_id']
    await db[col_name].update_one({"_id": _id}, {'$set': obj}, upsert=True)


async def download_books(api, gender, major, type, start=0, limit=50, total=-1):
    logger.debug('Download Page:%s, Size:%s  ----  gender(%s) , major (%s)' % (start, limit, gender, major))

    result = await api.books_by_cat(gender=gender, major=major, type=type, start=start, limit=limit)

    # api 请求会失败，有的时候第一个就失败，导致后面都没抓

    if result:

        if total == -1:
            total = result['total']

        for book in result['books']:

            existed = await db['book'].find_one({'_id': ObjectId(book['_id'])})

            if existed:
                logger.info('Skip %s' % book['title'])
            else:
                succeeded = await download_book(api, book, download_each_chapter=False)
                if succeeded:
                    book['gender'] = gender
                    await upsert('book', book)

        try:
            if total > start:
                await download_books(api, gender, major, type, start=start+limit, total=total)
        except Exception as e:
            logger.error('Api books_by_cat fail:%s, maybe no more results' % result)
            return


async def main():
    api = AsyncBookApi('https://api.zhuishushenqi.com', 'http://chapter2.zhuishushenqi.com')

    cats = await api.get_cats()
    top_cats = ['male', 'female', 'picture', 'press']
    print('Top cats: ' , top_cats)
    # press 是出版社


    # 空白有的能搜出来有的不行，搞不懂，全部试一遍
    types = ['', 'hot', 'new', 'repulation', 'over', 'month']

    # await download_books(api, 'press', '传记名著', 'hot', 0)

    # print([c for c in cats['press'] if c['name'] == '育儿健康'])
    # print(cats['press'][-1])
    # await download_books(api, 'press', '育儿健康', 'hot', 0)

    # 最后一个是育儿健康， 报错 应该是结束了吧
    # Traceback (most recent call last):
    # File "novel.py", line 392, in <module>
    #     loop.run_until_complete(future)
    # File "D:\Anaconda3\envs\py3\lib\asyncio\base_events.py", line 466, in run_until_complete
    #     return future.result()
    # File "novel.py", line 369, in main
    #     for c in v:
    # TypeError: 'bool' object is not iterable
    # ERROR:asyncio:Unclosed client session
    # client_session: <aiohttp.client.ClientSession object at 0x0000000004A6EB00>
    # ERROR:asyncio:Unclosed connector
    # connections: ['[(<aiohttp.client_proto.ResponseHandler object at 0x00000000037E3AC8>, 17202.089)]']
    # connector: <aiohttp.connector.TCPConnector object at 0x0000000004A6EB38>


    sub_cats = []
    tasks = []
    for k, v in cats.items():
        # sub_cats.extend(c['name'] for c in v)
        # if k == 'press':
        try:
            for c in v:
                for t in types:
                    await download_books(api, k, c['name'], t, 0)
        except TypeError:
            logger.error('TypeError, v:%s' % v)
            continue


mgclient = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
sem = asyncio.Semaphore(1000)
# session = ClientSession()
db = mgclient['bookdb']
book_col = db['book']
chapter_col = db['chapter']
chapter_col.create_index('link')



if __name__ == '__main__':
    

    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    
    # client = ClientSession(loop=loop)
    # future = asyncio.ensure_future(api.api_get('/cats/lv2/statistics'))
    future = asyncio.ensure_future(main())
    loop.run_until_complete(future)
    # session.close()

# api = BookApi('https://api.zhuishushenqi.com')
# ret = api.books_by_cat(gender='female', major='古代言情', type='hot', start=0, limit=20)
# import pdb

# pdb.set_trace()