import asyncio
import logging
import ssl
from typing import List
from datetime import datetime
import httpx
from airflow import XComArg
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook
from bs4 import BeautifulSoup
from sivillage.schema import SivillageSchema 

log = logging.getLogger()

class FetchStyleOperator(BaseOperator):
    url = "https://www.sivillage.com/dispctg/getCtgGoodsAjax.siv"


    def __init__(
        self,
        *,
        ctg_code: int,
        sort_types: List[int] = [120],
        max_page :int = 10 ,
        timeout: float = 320.0,
        max_concurrency: int = 30,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.ctg_code = ctg_code
        self.sort_types = sort_types
        self.max_page = max_page
        self.timeout = timeout
        self.max_concurrency = max_concurrency  # 동시 실행 개수 제한
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.base_payload ={
            "outlet_yn": "N",
            "disp_ctg_no":str(self.ctg_code),
        }
        self.product_num_per_page = 60
        self.cache_config = cache_config
        self.header = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://www.sivillage.com/dispctg/initDispCtg.siv?disp_ctg_no={self.ctg_code}",
        "Origin": "https://www.sivillage.com",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Connection" : "keep-alive",
        "Accept" :"*/*",
        "Accept-Encoding": "gzip, deflate, br",
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
        styles = el.run_until_complete(self.async_execute(context))
        return styles

    async def async_execute(self, context):
        tasks = []
        results = []
        for sort_type in self.sort_types :
            tasks.append(self.async_task(sort_type))
        parsed_response = await asyncio.gather(*tasks)
        for product_list in parsed_response:
            results.extend(product_list)
        return results

    async def async_task(self,sort_type):

        first_page_data = await self.fetch_post(self.url,payload= {**self.base_payload, "page_idx":"1",  "sort_type": str(sort_type)})
        soup = BeautifulSoup(first_page_data, 'html.parser')
        total_span  = soup.select_one("div.category__product--title h3 span")
        assert total_span , "None type err"
        total_products = int(total_span.text.replace(",", "")) # type: ignore
        total_page = total_products // self.product_num_per_page +1
        total_page = min(total_page,self.max_page)
        tasks = []
        results =[]
        for page in range(1,total_page+1):
            tasks.append(self.fetch_post(url=self.url, payload={**self.base_payload, "page_idx":str(page),  "sort_type": str(sort_type)}))
        responses = await asyncio.gather(*tasks)
        product_ids = map(self.extract_product_id , responses)
        for product_list in product_ids:
            results.extend(product_list)
        return results

    def extract_product_id(self, html_response):
        soup = BeautifulSoup(html_response, 'html.parser')
        product_ul = soup.find("ul", class_="product__list thum--4 gap--mid")
        product_items = product_ul.find_all('li', class_='product__item')  # type: ignore
        product_ids =  [ item.get('data-goods_no' ,None) for item in product_items ]
        return product_ids

    async def fetch_post(self, url, payload:dict,retries:int=0, max_retries:int=5, **kwargs) :
        log.info(payload)
        log.info(self.header)

        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.post(url, headers =self.header, data = payload)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(f"{url}, {e}")

            return await self.fetch_post(self.url, payload= payload ,retries=retries+1, exception=e)

        assert res.is_success, f"Failed to fetch data from {url} :: {res.status_code} :: {res.text}"
        return res.text



class FetchStyleInfoOperator(BaseOperator):
    url_template = "https://www.sivillage.com/goods/initDetailGoods.siv?goods_no={product_id}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    def __init__(
        self,
        *,
        styles: list[str],
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.styles = styles
        self.timeout = timeout
        self.max_concurrency = max_concurrency
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config = cache_config
        super().__init__(**kwargs)

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        ctg_gender_map = Variable.get(key="sivillage.categories.gender.map", default_var={}, deserialize_json=True)

        proxy = Variable.get(self.proxy_key)
        httpx_cache_transport =  AsyncMongoDBTransport(
            mongo_uri=mongo_uri, db=self.cache_config.db, collection=self.cache_config.collection, transport=httpx.AsyncHTTPTransport(
                retries=3, proxy=proxy
            )
        )
        self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        styles = asyncio.run(self.async_task())
        transformed_data = []

        for style_info in styles:
            style = self.transform_data(style_info, date=today, ctg_gender_map=ctg_gender_map )
            if style is not None:
                transformed_data.append(style.model_dump())
        return transformed_data

    async def async_task(self):
        async def task(style:str):

            url = self.url_template.format(product_id = style)
            try:
                product_html = await self.fetch_get(url)

            except IndexError:
                product_html = None

            return self.parse_html_style_info(product_html)
        tasks = [ task(style) for style in self.styles]
        product_htmls = await asyncio.gather(*tasks)
        return product_htmls
    
    def transform_data(self, style_info:dict, date:datetime, ctg_gender_map)-> SivillageSchema|None:
        self.log.debug(f"Processing style info of type {type(style_info)}: {style_info}")
        try:
            parsed_data = SivillageSchema.build(product_info=style_info, ctg_gender_map=ctg_gender_map ,date= date )
            return parsed_data
        except KeyError as e:
            self.log.error(f"Missing key in product info: {style_info}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {style_info}. Error: {e}")
            raise e
        
    def parse_html_style_info(self, html_data):
        soup = BeautifulSoup(html_data, 'html.parser')

        meta_tags = soup.find_all('meta')
        meta_data = {
        meta.get('property'): meta.get('content')
        for meta in meta_tags if meta.get('property') and meta.get('content')
    }
        temp_onclick_values = [div.get('onclick') for div in soup.select("div.siv-colorchip__border") if div.get('onclick')]
        onclick_values = temp_onclick_values if temp_onclick_values else []

        image_tags = soup.select("div.swiper-slide img")
        image_urls = [img['src'] for img in image_tags if img.get('src')]
        product_info = {"meta": meta_data, "variants": onclick_values, "images" : image_urls}
        return product_info

    async def fetch_get(self, url ,retries:int=0, max_retries:int=5, **kwargs) :
        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(f"{url}, {e}")

            return await self.fetch_get(url ,retries=retries+1, exception=e)

        assert res.is_success, f"Failed to fetch data from {url} :: {res.status_code} :: {res.text}"
        return res.text
