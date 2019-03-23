import requests
from requests.adapters import HTTPAdapter
from lxml import etree
import re
from urllib.parse import urlencode
import pymysql
import time
import random
import settings

# =====================================================================================================================
base_url = 'https://book.douban.com/tag/?view=type'
User_Agents = [
    'Mozilla/5.0(Macintosh;U;IntelMacOSX10_6_8;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50',
    'Mozilla/5.0(Windows;U;WindowsNT6.1;en-us)AppleWebKit/534.50(KHTML,likeGecko)Version/5.1Safari/534.50',
    'Mozilla/5.0(Macintosh;IntelMacOSX10.6;rv:2.0.1)Gecko/20100101Firefox/4.0.1',
    'Mozilla/5.0(WindowsNT6.1;rv:2.0.1)Gecko/20100101Firefox/4.0.1',
    'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;360SE)',
    'Opera/9.80(WindowsNT6.1;U;en)Presto/2.8.131Version/11.11']
User_Agent = random.choice(User_Agents)
headers = {
    'Host': 'book.douban.com',
    'User-Agent': User_Agent
}
table = settings.TABLE
host = settings.HOST
user = settings.USER
password = settings.PASSWORD
port = settings.PORT
database = settings.DATABASE
tags_list = ['出版社', '出版年', '页数', '定价', '装帧', '丛书', 'ISBN']
# =====================================================================================================================


class set_log(object):
    def loading_log(self, log_name):
        try:
            log_file = open(log_name)
            log_text = list(eval(log_file.read()))
        except BaseException:
            log_text = []
        return log_text

    def update_index_remain_type_and_book_url_list(
            self,
            index_type_url,
            index_log_name,
            remain_log_name,
            book_log_name):
        index_type_url_list.remove(index_type_url)
        index_record_text = index_type_url_list + ['index_record']
        remain_record_text = remain_type_url_list + ['remain_record']
        book_record_text = book_url_list + ['book_record']
        with open(index_log_name, "w") as index_record_file:
            index_record_file.write(str(index_record_text))
        with open(remain_log_name, "w") as remain_record_file:
            remain_record_file.write(str(remain_record_text))
        with open(book_log_name, "w") as book_record_file:
            book_record_file.write(str(book_record_text))

    def update_remain_type_and_book_url_list(
            self, remain_type_url, remain_log_name, book_log_name):
        remain_type_url_list.remove(remain_type_url)
        remain_record_text = remain_type_url_list + ['remain_record']
        book_record_text = book_url_list + ['book_record']
        with open(remain_log_name, "w") as remain_record_file:
            remain_record_file.write(str(remain_record_text))
        with open(book_log_name, "w") as book_record_file:
            book_record_file.write(str(book_record_text))

    def update_book_url_list(self, book_url, book_log_name):
        book_url_list.remove(book_url)
        book_record_text = book_url_list + ['book_record']
        with open(book_log_name, "w") as book_record_file:
            book_record_file.write(str(book_record_text))


class parse_data(object):
    def get_page(self, url):
        s = requests.Session()
        s.mount('http://', HTTPAdapter(max_retries=3))
        s.mount('https://', HTTPAdapter(max_retries=3))
        try:
            r = s.get(url, headers=headers, allow_redirects=False, timeout=3)
        except BaseException:
            raise
        return r.text

    def get_index_type_url(self, base_url):
        """从标签页获取所有标签链接"""
        base_response = self.get_page(base_url)
        base_html = etree.HTML(base_response)
        types = base_html.xpath(
            '//*[@id="content"]//div[@class="article"]//table[@class="tagCol"]//td/a/@href')
        types_base_url = 'https://book.douban.com'
        type_url_list = []
        for type in types:
            type_url = types_base_url + type
            type_url_list.append(type_url)
        return type_url_list
        time.sleep(3 + 3 * random.random())

    def get_remain_type_urls(
            self,
            index_type_url,
            index_log_name,
            remain_log_name,
            book_log_name,
            log):
        """从所有标签首页链接获取书本链接"""
        print(f"{index_type_url}开始爬取特定类型首页信息")
        db = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            db=database)
        type_response = self.get_page(index_type_url)
        type_html = etree.HTML(type_response)
        type_name = re.findall(r'\/tag\/(.*)', index_type_url)[0]
        book_sectors = type_html.xpath(
            '//*[@id="subject_list"]//li[@class="subject-item"]')
        max_page = ''
        for book_sector in book_sectors:
            comments_number_base = book_sector.xpath(
                './/div[@class="star clearfix"]/span[@class="pl"]/text()')[0]
            try:
                comments_number = re.findall(r'\d+', comments_number_base)[0]
            except BaseException:
                comments_number = 0
            try:
                score = book_sector.xpath(
                    './/div[@class="star clearfix"]/span[@class="rating_nums"]/text()')[0]
            except BaseException:
                score = '暂无评分'
            type_book_data = {
                'tag': type_name,
                'book_name': book_sector.xpath('./div[@class="info"]/h2/a/@title'),
                'score': score,
                'comments_number': comments_number}
            update_to_mysql(type_book_data, db)
            book_url = book_sector.xpath('./div[@class="info"]/h2/a/@href')[0]
            book_url_list.append(book_url)
        db.close()
        try:
            max_page = type_html.xpath(
                '//*[@id="subject_list"]//div[@class="paginator"]//a[last()]/text()')[0]
            for page in range(1, int(max_page)):
                params = {'start': str(page * 20),
                          'type': 'T'
                          }
                remain_type_url = index_type_url + '?' + urlencode(params)
                remain_type_url_list.append(remain_type_url)
            log.update_index_remain_type_and_book_url_list(
                index_type_url, index_log_name, remain_log_name, book_log_name)
        except BaseException:
            log.update_index_remain_type_and_book_url_list(
                index_type_url, index_log_name, remain_log_name, book_log_name)
        time.sleep(3 + 3 * random.random())

    def get_book_urls(
            self,
            remain_type_url,
            remain_log_name,
            book_log_name,
            log):
        """从所有标签首页以外链接获取书本链接"""
        print(f"{remain_type_url}开始爬取特定类型剩余信息")
        db = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            db=database)
        type_response = self.get_page(remain_type_url)
        type_html = etree.HTML(type_response)
        type_name = re.findall(r'\/tag\/(.*)\?', remain_type_url)
        book_sectors = type_html.xpath(
            '//*[@id="subject_list"]//li[@class="subject-item"]')
        book_urls = type_html.xpath(
            '//*[@id="subject_list"]//li[@class="subject-item"]/div[@class="info"]/h2/a/@href')
        for book_sector in book_sectors:
            comments_number_base = book_sector.xpath(
                './/div[@class="star clearfix"]/span[@class="pl"]/text()')[0]
            try:
                comments_number = re.findall(r'\d+', comments_number_base)[0]
            except BaseException:
                comments_number = 0
            try:
                score = book_sector.xpath(
                    './/div[@class="star clearfix"]/span[@class="rating_nums"]/text()')[0]
            except BaseException:
                score = '暂无评分'
            type_book_data = {
                'tag': type_name,
                'book_name': book_sector.xpath('./div[@class="info"]/h2/a/@title'),
                'score': score,
                'comments_number': comments_number}
            update_to_mysql(type_book_data, db)
            book_url = book_sector.xpath('./div[@class="info"]/h2/a/@href')[0]
            book_url_list.append(book_url)
        log.update_remain_type_and_book_url_list(
            remain_type_url, remain_log_name, book_log_name)
        db.close()
        time.sleep(3 + 3 * random.random())

    def get_book_data(self, book_url, book_log_name, log):
        """获取书本信息"""
        print(f"{book_url}开始爬取书本信息")
        book_data_string = ''
        db = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            db=database)
        book_response = self.get_page(book_url)
        book_html = etree.HTML(book_response)
        books_informations = book_html.xpath('//*[@id="info"]//text()')
        for books_information in books_informations:
            if books_information:
                book_data_string += books_information.strip().replace('\n', '').replace('\t',
                                                                                        '').replace('\r', '').replace(' ', '')
        book_data = {
            'book_name': book_html.xpath('//*[@id="wrapper"]/h1/span/text()'),
            'score': book_html.xpath('//*[@id="interest_sectl"]//strong[@class="ll rating_num "]/text()')[0].strip(),
            'author': re.findall(
                '作者:(.*?):',
                book_data_string)[0],
            'publishing_house': re.findall(
                '出版社:(.*?):',
                book_data_string)[0],
            'publish_year ': re.findall(
                '出版年:(.*?):',
                book_data_string)[0],
            'pages_number': re.findall(
                    '页数:(.*?):',
                    book_data_string)[0],
            'price': re.findall(
                        '定价:(.*?):',
                        book_data_string)[0],
            'ISBN': re.findall(
                            'ISBN:(.*)',
                book_data_string)[0]}
        for key, value in book_data.items():
            for i in range(0, len(tags_list)):
                if tags_list[i] in value:
                    book_data.update({key: value.strip(tags_list[i])})
        update_to_mysql(book_data, db)
        log.update_book_url_list(book_url, book_log_name)
        db.close()
        time.sleep(3 + 3 * random.random())


def update_to_mysql(data, db):
    """增量存放进mysql"""
    data_keys = ', '.join(data.keys())
    data_values = ', '.join(['%s'] * len(data))
    cursor = db.cursor()
    sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(
        table=table, keys=data_keys, values=data_values)
    update = ','.join([" {key} = %s".format(key=key) for key in data])
    sql += update
    cursor.execute(sql, tuple(data.values()) * 2)
    db.commit()
    print(f'{data} update success !')


def produce_url_list(url_list, log_list):
    lists = ''
    if len(log_list):
        x = set(url_list)
        y = set(log_list)
        lists = list(x & y)
    else:
        lists = url_list
    return lists


def run():
    global index_type_url_list, remain_type_url_list, book_url_list
    log = set_log()
    info = parse_data()
    index_type_url_list = log.loading_log('index_type_url.log')
    remain_type_url_log = log.loading_log('remain_type_url.log')
    remain_type_url_list = list(set(remain_type_url_log))
    book_url_log = log.loading_log('book_urls.log')
    book_url_list = list(set(book_url_log))
    type_url_list = info.get_index_type_url(base_url)
    index_type_url_list = produce_url_list(type_url_list, index_type_url_list)
    print(index_type_url_list)
    for index_type_url in index_type_url_list:
        try:
            info.get_remain_type_urls(
                index_type_url,
                'index_type_url.log',
                'remain_type_url.log',
                'book_urls.log',
                log)
        except BaseException:
            None
    print(remain_type_url_list)
    for remain_type_url in remain_type_url_list:
        try:
            info.get_book_urls(
                remain_type_url,
                'remain_type_url.log',
                'book_urls.log',
                log)
        except BaseException:
            None
    print(book_url_list)
    for book_url in book_url_list:
        try:
            info.get_book_data(book_url, 'book_urls.log', log)
        except BaseException:
            None


if __name__ == '__main__':
    run()
