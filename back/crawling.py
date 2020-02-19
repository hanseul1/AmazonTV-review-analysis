##### import & proxy setting

import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

##### proxy setting

url = ""
proxy_host = ""
proxy_port = ""
proxy_auth = ""
proxies = {"https": "https://{}@{}:{}/". format(proxy_auth, proxy_host, proxy_port),
      "http": "http://{}@{}:{}/". format(proxy_auth, proxy_host, proxy_port)}

r = requests.get(url, proxies=proxies, verify=False)

print(r.text)

##### elasticsearch setting

host = '' #without 'https'
YOUR_ACCESS_KEY = ''
YOUR_SECRET_KEY = ''
REGION = '' #change to your region
awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=False,
    connection_class=RequestsHttpConnection
)
print(es.info())

# Model data crawling

model_url_list = [
    'https://www.amazon.com/Samsung-UN50RU7100FXZA-FLAT-UHD-Smart/dp/B07NC9SPBF/ref=sr_1_4?keywords=tv&qid=1576055290&smid=ATVPDKIKX0DER&sr=8-4&th=1',
    'https://www.amazon.com/TCL-75R617-75-Inch-Ultra-Smart/dp/B07M8D8JDK/ref=sr_1_52?keywords=tv&qid=1576056774&smid=ATVPDKIKX0DER&sr=8-52&th=1',
    'https://www.amazon.com/VIZIO-D40-D1-Class-Array-Smart/dp/B01A0LGS3Q/ref=sr_1_41?keywords=tv&qid=1576056774&sr=8-41',
    'https://www.amazon.com/Samsung-Electronics-Smart-LED-UN55NU6900FXZA/dp/B07F26ZQ8L/ref=sr_1_17?keywords=tv&qid=1576055290&sr=8-17'
]

for i in range(0, len(model_url_list)) :
    base_url = model_url_list[i]
    model_id = re.findall(r'B[\w\.-]+', base_url)
    price_url = 'https://www.amazon.com/gp/offer-listing/' + model_id[0] + '/?ie=UTF8&condition=new'
    
    # model info crawling
    page = requests.get(base_url, verify=False).text
    soup = BeautifulSoup(page, "html.parser")
    
    desc_table = soup.find_all('table', 'prodDetTable')
    model_code = ''
    brand = ''
    rating = float(soup.find('span', 'reviewCountTextLinkedHistogram').get('title').split()[0])
    product_name = soup.find('span', {'id' : 'productTitle'}).text
    
    for tr in desc_table[0].find_all('tr'):
        th = tr.find('th').text.strip()
        if th == 'Brand Name':
            brand = tr.find('td').text
        elif th == 'Item model number':
            model_code = tr.find('td').text
    
    # price info crawling
    page = requests.get(price_url, verify=False).text
    soup = BeautifulSoup(page, "html.parser")
    
    price = re.sub(',', '', soup.find('span', 'olpOfferPrice').text.strip())
    
    doc = {
        "model_id": model_id[0],
        "model_code": model_code.strip(),
        "product_name": product_name.strip(),
        "brand": brand.strip(),
        "price": float(price[1:]),
        "rating": rating
    }
    
    res = es.index(index="models", doc_type='model_info', id=model_id[0], body=doc)
    
# Review data crawling & es put

url_list = [
    'https://www.amazon.com/Samsung-UN50RU7100FXZA-50-Inch-Ultra-Compatibility/product-reviews/B07NC9SPBF/ref=cm_cr_arp_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&pageNumber='
    ,'https://www.amazon.com/TCL-75R617-75-Inch-Ultra-Smart/product-reviews/B07M8D8JDK/ref=cm_cr_arp_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&pageNumber='
    ,'https://www.amazon.com/VIZIO-D40-D1-Class-Array-Smart/product-reviews/B01A0LGS3Q/ref=cm_cr_arp_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&pageNumber='
    'https://www.amazon.com/Samsung-Electronics-Smart-LED-UN55NU6900FXZA/product-reviews/B07F26ZQ8L/ref=cm_cr_arp_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&pageNumber='
]

# model loop
for i in range(0, len(url_list)):
    base_url = url_list[i]
    model_id = re.findall(r'B+[\w\.-]+', base_url)
    pageNumber = 1
    
    # pagination loop
    while True:
        pageurl = base_url + str(pageNumber)
        page = requests.get(pageurl, verify=False).text
        soup = BeautifulSoup(page, "html.parser")
        
        reviews = soup.find_all('div', 'review')

        if reviews == [] : 
            break

        # reviews loop(1 page per 10 reviews)
        for j in range(0, len(reviews)-1):
            review = {}
            review_id = reviews[j].get('id')
            review_text = reviews[j].find('span', 'review-text').text
            review_rating = float(reviews[j].find('i', class_= "review-rating").text.split()[0])
            review_title = reviews[j].find('a', 'review-title').text
            review_date = reviews[j].find('span', 'review-date').text
            review_date = datetime.strptime(review_date, '%B %d, %Y').date()

            doc = {
                "review_id" : review_id,
                "title" : review_title.strip(),
                "rating" : review_rating,
                "date" : review_date,
                "content" : review_text.strip(),
                "model_id" : model_id[0]
            }

            res = es.index(index="reviews", doc_type='review_info', id=review_id, body=doc)

        pageNumber += 1
        print(res['result'])

models = ["B07F26ZQ8L", "B07M8D8JDK"]
model_id = ''
for model in models:
    model_id += model + ' '

frompage = 0

query_body = {
  "from" : frompage,
  "size" : 10,
  "sort" : {"date" : "desc"},
  "query": {
    "match" : {
      "model_id" : model_id
    }
  }
}

review_cnt = res['hits']['total']['value']

res = es.search(index="reviews", body= query_body)
print("Got %d Hits:" % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    print("%s" % hit["_source"])