import asyncio
from functools import reduce
import logging
import re
import ssl
from typing import List

import httpx
from airflow import XComArg
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook
from urllib3._collections import HTTPHeaderDict
from bs4 import BeautifulSoup
from ssf.schema import  SsfSchema
from datetime import datetime

log = logging.getLogger()

class FetchStyleOperator(BaseOperator):
    url_tamplate= "https://www.ssfshop.com/selectProductList?dspCtgryNo={ctg_code}&currentPage={page_num}&sortColumn={sort_type}&serviceType=DSP&ctgrySectCd=GNRL_CTGRY&fitPsbYn=N"


    def __init__(
        self,
        *,
        ctg_code: str,
        sort_type : str  = "SALE_QTY_SEQ",
        timeout: float = 320.0,
        ctg_gender_map :dict,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.ctg_code = ctg_code
        self.ctg_gender_map =ctg_gender_map
        self.timeout = timeout
        self.sort_type = sort_type
        self.max_concurrency = max_concurrency  # 동시 실행 개수 제한
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config = cache_config
        self.header = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Connection" : "keep-alive",
        }

        super().__init__(**kwargs)

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)

        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri, db=self.cache_config.database, collection=self.cache_config.collection, today=today, transport=httpx.AsyncHTTPTransport(retries=3,proxy=proxy)
        )
        
        self.aclient= httpx.AsyncClient(headers=self.header, timeout=self.timeout, transport=httpx_cache_transport, follow_redirects=True)
        el = asyncio.get_event_loop()
        transformed_data = []

        styles = el.run_until_complete(self.async_execute(context))
        for style_info in styles :
            style = self.transform_data(style_info, date=today)
            if style is not None:
                transformed_data.append(style.model_dump())
        return transformed_data
    
    def transform_data(self, style_info:dict, date:datetime)-> SsfSchema|None:
        self.log.debug(f"Processing style info of type {type(style_info)}: {style_info}")
        try:
            parsed_data = SsfSchema.build(product_data = style_info,category_mapping=self.ctg_gender_map  ,date= date)
            return parsed_data
        except KeyError as e:
            self.log.error(f"Missing key in product info: {style_info}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {style_info}. Error: {e}")
            raise e
    async def async_execute(self, context):
        
        parsed_response = await self.async_task(self.sort_type)
        log.info(f"parsed_response: {parsed_response}")    
        return parsed_response

    async def async_task(self,sort_type):
        url  = self.url_tamplate.format(ctg_code = self.ctg_code, page_num = 1,sort_type= sort_type )
        first_page_data = await self.fetch_get(url)
        soup = BeautifulSoup(first_page_data, 'html.parser')
        total_page_tag  = soup.find('a', id='page_last')
        log.info(f"total page:{total_page_tag} ")

        assert total_page_tag , "None type err"
        total_page =total_page_tag.get("pageno") # type: ignore
        log.info(f"original total page:{total_page} ")
        
        # test를 위해 clipping
        total_page = min(10, int(total_page)) # type: ignore 
        
        tasks = []
        result = []
        for page in range(1, int(total_page)): # type: ignore
            url  = self.url_tamplate.format(ctg_code = self.ctg_code, page_num =page,sort_type= sort_type )
            tasks.append(self.fetch_get(url=url))
        responses = await asyncio.gather(*tasks)
        product_list = [self.extract_product_info(response) for response in responses]
        log.info(product_list)
        for parsed_products in product_list:
            result.extend(parsed_products)
        log.info(f"results: {result}")
        return result

    def extract_product_info(self, html_response):
        soup = BeautifulSoup(html_response, 'html.parser')
        result = []
        for li in soup.find_all('li', attrs={'data-prdno': True}):
            prdno = li.get('data-prdno')
            img_main = li.find("div", class_= "god-img")
            image_urls =[img_tag["src"] for img_tag in img_main.find_all("img")]

            brand_tag = li.find('span', class_='brand')
            name_tag = li.find('span', class_='name')
            brand = brand_tag.get_text(strip=True) if brand_tag else ''
            name = name_tag.get_text(strip=True) if name_tag else ''
            
            price_info = li.find('span', class_='price').text
            
            result.append({
                "style_id": prdno,
                "brand": brand,
                "name": name,
                "price_info": price_info,
                "ctg_code" :self.ctg_code,
                "image_urls": image_urls
            }) 

        log.info(f"parsed result {result}")

        return result

    async def fetch_get(self, url, retries:int=0, max_retries:int=5, **kwargs) :

        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        
        try:
            async with self.semaphore:
                res = await self.aclient.get(url, headers =self.header)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(f"{url}, {e}")

            return await self.fetch_get(url=url,retries=retries+1, exception=e)

        assert res.is_success, f"Failed to fetch data from {url} :: {res.status_code} :: {res.text}"
        return res.text

