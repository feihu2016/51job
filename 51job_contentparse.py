#encoding=utf-8

import requests,time,re,json,sys,os
import asyncio
import gzip
from io import StringIO
import urllib3

global_path = "/data/server/www/haozu/haozu_online/crawler/"
sys.path.append(global_path)

import random,redis 
from lxml import html
import MySQLdb
from misc.helpers import *
from misc.agents import AGENTS
from misc.config import DB_SERVER, DB_CONNECT

def random_proxy():
    length = len(AGENTS)
    index  = random.choice(range(0,length))
    return AGENTS[index]

def retstr(listi, index = 0, default = ''): 
    if isinstance(listi, list) and len(listi) > index:
        return listi[index].strip().replace("'", "").replace("\t", "").replace(" ", "")
    return default

def contentstr(listi, default = '<br/>'):
    if isinstance(listi, list):
        return '<br/>'.join(listi).replace("'", "").replace("\t", "")
    else:
        return ''


def restart_net():
    print(time.strftime("%Y-%m-%d %X", time.localtime()),'网络异常，重新拨号....')
    sys.stdout.flush()
    os.system('pppoe-stop')
    time.sleep(3)
    os.system('pppoe-start')
    time.sleep(10)
    return


async def get_urls(url, headers, timeout, redis_cur):
    try:
        http = urllib3.PoolManager(timeout = 30)
        res = http.request('GET', url, headers=headers)
        if res.status == 200:
            return res.data
        else:
            print(time.strftime("%Y-%m-%d %X", time.localtime()), '解析失败，请求码：%s' % res.status)
            sys.stdout.flush()
            return ''

    except requests.exceptions.ConnectTimeout:
        print(time.strftime("%Y-%m-%d %X", time.localtime()), 'Connect  超时!')
        sys.stdout.flush()
        redis_cur.set('51job', 'off')
        return 
    except requests.exceptions.Timeout:
        print(time.strftime("%Y-%m-%d %X", time.localtime()), 'Connect  异常!')
        sys.stdout.flush()
        redis_cur.set('51job', 'off')
        return
    except Exception as e:   #捕捉网络错误
        print(time.strftime("%Y-%m-%d %X", time.localtime()), 'Connect  异常!')
        sys.stdout.flush()
        redis_cur.set('51job', 'off')
        return

async def request_content(url, mysql_cur, redis_cur, data_id):
    print(time.strftime("%Y-%m-%d %X", time.localtime()), url, '开始解析....')
    agentheader = random_proxy()
    headers = {'content-type': 'text/html',
               'Accept-Encoding': 'gzip, deflate',
               'User-Agent': agentheader,
    }
    body_text = await get_urls(url, headers, 30, redis_cur)
    response = ''
    if body_text:
        #response = html.fromstring(gzip.GzipFile(fileobj=StringIO(body_text.decode('gbk'))).read().decode('gbk'))
        response = html.fromstring(body_text.decode('gbk'))
        #response = html.fromstring(res.text) 
        item = {}
        title = retstr(response.xpath("//p[@class='ltype']/text()"))
        item['company_size'] = ''      #公司规模
        item['company_nature'] = ''    #公司性质,合资、国企...
        item['company_industry'] = ''  #公司行业
        item['company_address'] = retstr(response.xpath(u"//span[contains(text(),'公司地址')]/following-sibling::text()"))   #公司地址
        title_list = title.split('|')
        try:
            item['company_nature'] = title_list[0]
            item['company_size'] = title_list[1]
            item['company_industry'] = title_list[2]
        except:
            pass

        item['introduce'] = contentstr(response.xpath("//div[@class='con_msg']/div/p/text()"))
        item['data_id'] = data_id   
        item['updated_at'] = int(time.time())
        update_sql = "update ws_company_info_18 set company_size='%(company_size)s',company_nature='%(company_nature)s',company_industry='%(company_industry)s',company_address='%(company_address)s',updated_at=%(updated_at)s,introduce='%(introduce)s' where id=%(data_id)s" % item
        mysql_cur.execute(update_sql)
        print('---------------------update: %s-------------------------' % time.strftime("%Y-%m-%d %X", time.localtime()))
        print(update_sql)
        sys.stdout.flush()
    else:
        print(time.strftime("%Y-%m-%d %X", time.localtime()), '未获取数据!', body_text)
    return


#geturl = "http://sh.xzl.anjuke.com/zu/shijigongyuan/"
if __name__ == "__main__":
    total = 1
    while True:
        red = redis.Redis(host='localhost',port=6379,db=0)
        get_51job = red.get('51job')
        if get_51job:
            if get_51job.decode('utf-8') == 'off':
                restart_net()
                red.set('51job', 'on')

        conn = MySQLdb.connect(host='localhost',port=3306,user='root',passwd='123456',charset='utf8')
        conn.autocommit(1)
        cur = conn.cursor()
        cur.execute('use crawl')
        #request_content('test', cur, 10)
        #exit()
        cur.execute('select id,url from ws_company_info_18 where crawled_at=0 order by id asc limit 10')
        if cur.rowcount>0:
            tasks = list(range(cur.rowcount))
            loop = asyncio.get_event_loop()
            num = 0
            for row in cur.fetchall():
                this_url = '%s#%s' % (row[1], row[0])
                cur.execute('update ws_company_info_18 set crawled_at=%s where id=%s' % (int(time.time()), row[0]))
                tasks[num] = asyncio.ensure_future(request_content(this_url, cur, red, row[0]))
                num += 1
            total += num
            loop.run_until_complete(asyncio.wait(tasks))
            print(time.strftime("%Y-%m-%d %X", time.localtime()),'%s 条执行完毕！' % total)
            sys.stdout.flush()
            cur.close()
            conn.close()
        else:
            cur.close()
            conn.close()
            print(time.strftime("%Y-%m-%d %X", time.localtime()),'爬取完毕！')
            sys.stdout.flush()
            exit()

