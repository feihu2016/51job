#encoding=utf-8

import requests,time,re,json,sys,os
import urllib2
import gzip, cStringIO

reload(sys)
sys.setdefaultencoding('utf-8')
global_path = "/data/server/www/haozu/haozu_online/crawler/"
sys.path.append(global_path)

from misc.agents import AGENTS
import random 
from lxml import html
import MySQLdb
from misc.helpers import *

def random_proxy():
    length = len(AGENTS)
    index  = random.choice(range(0,length))
    return AGENTS[index]


def request_list(url = '',sleep = 0,num = 0,deep = 0, cookies = {}):
    agentheader = random_proxy()
    headers = {'content-type': 'text/html',
           'User-Agent': agentheader,
    }

    res = None
    try:
        req = urllib2.Request(url, headers=headers)
        req.add_header('Accept-Encoding', 'gzip, deflate')
        res = urllib2.urlopen(req, timeout=30)
        #res = requests.get(url,headers=headers,cookies=cookies,timeout=6)
        response = html.fromstring(gzip.GzipFile(fileobj=cStringIO.StringIO(res.read() )).read().decode('gbk'))
        #for k,v in zip(res.cookies.keys(),res.cookies.values()):                                                             
        #    cookies[k] = v

    except Exception as e:   #捕捉网络错误
        #print geturl
        print(time.strftime("%Y-%m-%d %X", time.localtime()), url, '网络异常，重新拨号....')
        sys.stdout.flush()
        os.system('pppoe-stop')
        time.sleep(3)
        os.system('pppoe-start')
        time.sleep(10)
        deep += 1
        request_list(url,sleep,num,deep)

    #yanzhengma = retstr_replace(response.xpath(u'//*/div[@class="verify_info"]/@class'))
    #if res.status_code == 200:   # and not yanzhengma:
    if res.code == 200:
        listurl = response.xpath(u"//span[@class='t2']/a/@href")
        company_name = response.xpath(u"//span[@class='t2']/a/text()")
        pubdate = response.xpath(u"//span[@class='t5']/text()")
        print(time.strftime("%Y-%m-%d %X", time.localtime()), len(listurl), url)
        sys.stdout.flush()
        conn = MySQLdb.connect(host='localhost',port=3306,user='root',passwd='123456',charset='utf8')
        conn.autocommit(1)
        cur = conn.cursor()
        cur.execute('use crawl')
        cur.execute('set names utf8')

        for con_url,con_company,con_pubdate in zip(listurl, company_name, pubdate[1:]):
            #con_url = 'http://bj.fangtan007.com' + con_url
            cur.execute("select * from ws_company_info_18 where url like '%s'" % (con_url))
            if cur.rowcount < 1:
                con_pubdate = '2018-' + con_pubdate
                insert_sql = "insert into ws_company_info_18(website_id,company_name,url,city_id,city_name,created_at,pubdate) values(18,'%s','%s',%s,'%s',%s,'%s')" % (con_company,con_url,12,u'北京',time.time(),con_pubdate)
                try:
                    cur.execute(insert_sql)
                except:
                    print('error: %s' % insert_sql)
                print('insert URL:',con_url)
                sys.stdout.flush()
            else:
                print('repeat URL:',con_url)
                sys.stdout.flush()

        cur.close()
        conn.close()
        next_page = retstr_replace(response.xpath(u"//li[@class='bk'][2]/a/@href"))
        #if next_page and num<4:    #调试用
        if next_page and num<500:
            #next_page = 'http://bj.fangtan007.com' + next_page
            print(time.strftime("%Y-%m-%d %X", time.localtime()), '爬取深度报告:', next_page, 'sleep:', sleep, 'num:', num, 'deep:', deep)
            sys.stdout.flush()
            num += 1
            deep += 1
            time.sleep(sleep)
            request_list(next_page,sleep,num,deep,cookies)
            return
        else:
            print(time.strftime("%Y-%m-%d %X", time.localtime()), '爬取完毕!')
            sys.stdout.flush()
            res.close()
            return

    else:   #捕捉302 404跳转
        print(time.strftime("%Y-%m-%d %X", time.localtime()), url, '302屏蔽，重新拨号....')
        sys.stdout.flush()
        if res:
            res.close()
        os.system('pppoe-stop')
        time.sleep(3)
        os.system('pppoe-start')
        time.sleep(10)
        deep += 1
        request_list(url,sleep,num,deep)


if __name__ == "__main__":
    geturl = [
    #北京 - 外资
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=01&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 外资（非欧美）
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=02&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 合资
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=03&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 国企
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=04&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 外企代表处
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=06&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 政府机关
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=07&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 事业单位
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=08&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 非盈利组织
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=09&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 上市公司
    #'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=10&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    #北京 - 创业公司 
    'http://search.51job.com/list/010000,000000,0000,00,9,99,%25E5%2585%25AC%25E5%258F%25B8,2,1.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=11&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=8&dibiaoid=0&address=&line=&specialarea=00&from=&welfare=',
    ]
    for url in geturl:
        request_list(url,3,0)


