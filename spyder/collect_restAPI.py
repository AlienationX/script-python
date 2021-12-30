# coding:utf-8
# python3

import requests
import json
from lxml import etree

url = "http://gycq.zjw.beijing.gov.cn/enroll/dyn/enroll/viewEnrollHomePager.json"

headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Cookie": "JSESSIONID=D2BAEA8436794A2C5AF01617E599CAF1; _trs_uv=kv7w8o4o_365_42hq; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2217cbbf51ebd2b0-05f71c95600a88-5f1d351c-1049088-17cbbf51ebe66a%22%7D; session_id=98e1304c-4ff2-4c3b-b939-edd0a0e275e4; _va_ref=%5B%22%22%2C%22%22%2C1639641434%2C%22http%3A%2F%2Fgycq.zjw.beijing.gov.cn%2F%22%5D; _va_id=f36284c02ae52452.1632360147.14.1639641434.1639641434.; _va_ses=*",
    # "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36",
}

payload = {
    "currPage": 1,
    "pageJSMethod": "goToPage"
}

response = requests.post(url=url, headers=headers, data=json.dumps(payload))  # 2.4.2 版开始可以不用使用json.dumps传入

result = response.json()

# print(json.dumps(result, indent=4))
# print(result["data"])

# 生成选择器对象
dom = etree.HTML(result["data"])
# print(etree.tounicode(dom, pretty_print=True))

projects = dom.xpath("//table")
# print(projects[0].tag)

for i in range(1, len(projects) + 1):
    title = dom.xpath(f"//table[{i}]/caption/a/text()")[0] + dom.xpath(f"//table[{i}]/caption/text()")[0]
    # print(title)
    bodys = dom.xpath(f"//table[{i}]/tbody/tr")
    for j in range(1, len(bodys) +1 ):
        cols = dom.xpath(f"//table[{i}]/tbody/tr[{j}]/th")
        for k in range(1, len(cols) + 1):
            key = dom.xpath(f"//table[{i}]/tbody/tr[{j}]/th[{k}]/text()")
            val = dom.xpath(f"//table[{i}]/tbody/tr[{j}]/td[{k}]/text()")
            print(key[0] + val[0])
    print()

print("详情链接：http://gycq.zjw.beijing.gov.cn/enroll/home.jsp")
