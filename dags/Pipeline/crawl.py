import requests
import time
import random
import json
import pandas as pd
import urllib.parse
from datetime import datetime
import boto3
from io import StringIO
import logging

def crawl_id (**kwargs):
    cookies ={'_trackity': 'b21ec650-f58e-0ef8-69cd-146daaafce16', 'TOKENS': '{"access_token":"k7tngCXfPWeQ8qUd1ou9JDF3HcblZ2zw","expires_in":157680000,"expires_at":1871714875011,"guest_token":"k7tngCXfPWeQ8qUd1ou9JDF3HcblZ2zw"}', 'delivery_zone': 'Vk4wMzQwMjQwMTM=', '_ga': 'GA1.1.1103919319.1714034874', '_gcl_au': '1.1.1680218575.1714034877', 'tiki_client_id': '1103919319.1714034874', '_hjSessionUser_522327': 'eyJpZCI6ImQ1NGIzN2JiLTNmYzYtNWM2ZC05MTJkLTM4ZjhjOTQzNDJmZCIsImNyZWF0ZWQiOjE3MTQwMzQ4NzcxNzYsImV4aXN0aW5nIjp0cnVlfQ==', '_fbp': 'fb.1.1714034882503.625008211', '__uidac': '01662a18c28a1e80e11df656ba7292a6', 'dtdz': 'd8ce3089-7368-50a7-8959-78689e2e859a', '__iid': '749', '__su': '0', 
                '__RC': '4', '__R': '1', '__tb': '0', 'cto_bundle': 'Nn0BUF9YbzhCZjVGbUtENzVEVkZDdzNhZTlRMXdJS1N2TXpiaEZ4SURhWVY5aElaY21SNFZNUGY1V2lyOU9SZ0RjaTdObDklMkJ6JTJCa3klMkZVdmVUaTRnUHFzSnRucWtqaVc5WDlldUkxRVA5T09hbUdpMDgwRXJ3N0Z5T1R2Wk5IREVWUmZVRU9lcTBhUmNmdjJYTUd5TDhMJTJCckY4USUzRCUzRA', 'TIKI_RECOMMENDATION': 'c0aa32266cd6b80aa0653d6c4d071958', 'TKSESSID': 'e4f685885073b97e3401415209ee4569', '__adm_upl': 'eyJ0aW1lIjoxNzE1MDY2ODcyLCJfdXBsIjoiMC01OTE0MDM0ODgyNjI4ODQyNTk5In0=', '_hjSession_522327': 'eyJpZCI6Ijk5MDdlNDQzLTQ3MWQtNGE1NC04MzhjLTRkNTJjYjljNTE0ZiIsImMiOjE3MTUwNjUwNzE4NjcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=', '__IP': '3741114323', '__uif': '__uid:5914034882628842599|__ui:1%2C3|__create:1714034884', '_ga_S9GLR1RQFJ': 'GS1.1.1715065067.3.1.1715065278.26.0.0', 'amp_99d374': 'mqNpo1HyAgcx5EdGLas4P8...1ht8tg5vn.1ht8tmkr1.1k.2b.3v'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en,vi-VN;q=0.9,vi;q=0.8,fr-FR;q=0.7,fr;q=0.6,en-US;q=0.5',
        'Referer': 'https://tiki.vn/nha-sach-tiki/c8322',
        'x-guest-token': 'U1NEWPl5BkYObCZd6rH8tcopw7Q9eVDS',
        'Connection': 'keep-alive',
        'TE': 'Trailers',
    }

    params = {
        'limit': '40',
        'include': 'advertisement',
        'aggregations': '2',
        'trackity_id': 'b21ec650-f58e-0ef8-69cd-146daaafce16',
        'category': '8322',
        'page': '1',
        'src': 'c8322',
        'urlKey':  'nha-sach-tiki',
    }
    data = []
    for i in range(1, 10):
        params['page'] = i
        response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings', headers=headers, params=params, cookies=cookies)
        if response.status_code == 200:
            for record in response.json().get('data'):
                values = {
                    'product_id': record.get('id'),
                    'product_spid': record.get('url_path').split('=')[-1]
                }
                data.append(values)
        else:
            break
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    return 'OK'



def crawl_product_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='crawl_id')
    data = json.loads(data)
    df_id = pd.DataFrame(data)
    product_id = df_id['product_id'].to_list()
    product_spid = df_id['product_spid'].to_list()
    
    cookies = {'_trackity': 'b21ec650-f58e-0ef8-69cd-146daaafce16', 'TOKENS': '{"access_token":"k7tngCXfPWeQ8qUd1ou9JDF3HcblZ2zw","expires_in":157680000,"expires_at":1871714875011,"guest_token":"k7tngCXfPWeQ8qUd1ou9JDF3HcblZ2zw"}', 'delivery_zone': 'Vk4wMzQwMjQwMTM=', '_ga': 'GA1.1.1103919319.1714034874', '_gcl_au': '1.1.1680218575.1714034877', 'tiki_client_id': '1103919319.1714034874', '_hjSessionUser_522327': 'eyJpZCI6ImQ1NGIzN2JiLTNmYzYtNWM2ZC05MTJkLTM4ZjhjOTQzNDJmZCIsImNyZWF0ZWQiOjE3MTQwMzQ4NzcxNzYsImV4aXN0aW5nIjp0cnVlfQ==', '_fbp': 'fb.1.1714034882503.625008211', '__uidac': '01662a18c28a1e80e11df656ba7292a6', 'dtdz': 'd8ce3089-7368-50a7-8959-78689e2e859a', '__iid': '749', '__su': '0', 
    '__RC': '4', '__R': '1', '__tb': '0', 'TIKI_RECOMMENDATION': 'c0aa32266cd6b80aa0653d6c4d071958', 'TKSESSID': 'e4f685885073b97e3401415209ee4569', '__adm_upl': 'eyJ0aW1lIjoxNzE1MDY2ODcyLCJfdXBsIjoiMC01OTE0MDM0ODgyNjI4ODQyNTk5In0=', '_hjSession_522327': 'eyJpZCI6Ijk5MDdlNDQzLTQ3MWQtNGE1NC04MzhjLTRkNTJjYjljNTE0ZiIsImMiOjE3MTUwNjUwNzE4NjcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=', '__IP': 
    '3741114323', 'amp_99d374': 'mqNpo1HyAgcx5EdGLas4P8...1ht8tg5vn.1ht8u314c.27.38.5f', 'cto_bundle': 'qj6frF9YbzhCZjVGbUtENzVEVkZDdzNhZTlmcHlWbzhyUHBKZUV2T0tlQ3RUcGFYSGZHejVjZlZmdVRiR1BvQjJpTDc5dU96TGRNWFc1dzhCUXNzRGRWR0FmMW55QWNXOW03UXUySSUyRm91ODRNNVBPM2FIUEdZdnh3U1JSd3lkU1ljN1M2TUhWMldpOWw0cFd5Wkt1clZ5REd4TFJ3Sk51VVl4UGpFSzIweVo2ZGcyS3Y2ekpvcncxcTJXQk9sYUpyMTFrdE5TTnRId3pyanpvS0l2ZkJJOWFUYiUyRkYwcmk0M25GdVklMkJWSUhscWt3MFpzVEpBSzhhUldZaXoyZW5LWXZyRWxz', '__uif': '__uid:5914034882628842599|__ui:1%2C3|__create:1714034884', '_ga_S9GLR1RQFJ': 'GS1.1.1715065067.3.1.1715065728.60.0.0'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en,vi-VN;q=0.9,vi;q=0.8,fr-FR;q=0.7,fr;q=0.6,en-US;q=0.5',
        'Referer': 'https://tiki.vn/nha-sach-tiki/c8322',
        'x-guest-token': 'k7tngCXfPWeQ8qUd1ou9JDF3HcblZ2zw',
        'Connection': 'keep-alive',
        'TE': 'Trailers',
}
    
    result = []
    for i in range(0,len(product_id)):
        response = requests.get('https://tiki.vn/api/v2/products/{}?platform=web&spid={}&version=3'.format(product_id[i], product_spid[i]), headers=headers, cookies=cookies)
        if response.status_code == 200:
            d = dict()
            d['id'] = response.json().get('id')
            d['sku'] = response.json().get('sku')
            d['name'] = response.json().get('name')
            d['all_time_quantity_sold'] = response.json().get('all_time_quantity_sold')
            d['price'] = response.json().get('current_seller').get('price')
            d['store_id'] = response.json().get('current_seller').get('store_id')
            d['description'] = response.json().get('short_description')
            d['discount'] = response.json().get('discount')
            d['discount_rate'] = response.json().get('discount_rate')
            d['gift_item_title'] = response.json().get('gift_item_title')
            d['images'] = response.json().get('images')[-1].get('base_url')
            d['inventory_status'] = response.json().get('inventory_status')
            d['list_price'] = response.json().get('list_price')
            d['original_price'] = response.json().get('original_price')
            d['rating_average'] = response.json().get('rating_average')
            d['review_count'] = response.json().get('review_count')
            result.append(d)
            time.sleep(random.randrange(1, 3))
    json_rows = json.dumps(result)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    return 'OK'



def clean_write_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='crawl_product_data')
    data = json.loads(data)
    df = pd.DataFrame(data)
    df = df.drop(columns=['sku', 'gift_item_title', 'description'])
    df= df.fillna(0)
    df['inventory_status'] = [1 if x == 'available' else 0 for x in df['inventory_status']]

    file_name = ('product_tiki' + str(datetime.now().date())
                    + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    session = boto3.Session(
    aws_access_key_id='your_key',
    aws_secret_access_key='your_secret_key',
    )
    s3_res = session.resource('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    bucket_name = 'tikicrawler'
    s3_object_name = file_name
    s3_res.Object(bucket_name, s3_object_name).put(Body=csv_buffer.getvalue())

    return 'OK'