# import asyncio
# from functools import reduce
# import logging
# import ssl
# from typing import List

# import httpx
# from airflow import XComArg
# from airflow.models.baseoperator import BaseOperator
# from airflow.utils.context import Context
# from airflow.models.variable import Variable
# from core.infra.httpx_cache.mongo import AsyncMongoDBTransport
# from airflow.providers.mongo.hooks.mongo import MongoHook
# from urllib3._collections import HTTPHeaderDict
# from bs4 import BeautifulSoup

# class FetchRankOperator(BaseOperator):
#     url = "https://www.sivillage.com/dispctg/getCtgGoodsAjax.siv"


#     def __init__(
#         self,
#         *,
#         ctg_code: str,
#         sort_types: List[int] = [120],
#         max_page :int = 10 ,
#         timeout: float = 320.0,
#         max_concurrency: int = 50,
#         proxy_key:str = "smartproxy.kr",
#         mongo_conn_id:str ="ncp-mongodb",
#         **kwargs,
#     ):
#         self.ctg_code = ctg_code
#         self.sort_types = sort_types
#         self.max_page = max_page
#         self.timeout = timeout
#         self.max_concurrency = max_concurrency  # 동시 실행 개수 제한
#         self.proxy_key = proxy_key
#         self.mongo_conn_id = mongo_conn_id
#         self.semaphore = asyncio.Semaphore(max_concurrency)
#         self.base_payload ={
#             "outlet_yn": "N",
#             "disp_ctg_no": str(self.ctg_code),
#             "disp_clss_cd": 10
#         }
#         self.product_num_per_page = 60

#         self.header = {
#         "User-Agent": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#             "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
#         ),
#         "Referer": f"https://www.sivillage.com/dispctg/initDispCtg.siv?disp_ctg_no={self.ctg_code}",
#         "Origin": "https://www.sivillage.com",
#         "Content-Type": "application/json",
#     }

#         super().__init__(**kwargs)

#     def execute(self, context: Context):
#         today = context['execution_date'].in_timezone("UTC").today() # type: ignore
#         mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
#         proxy = Variable.get(self.proxy_key)

#         httpx_cache_transport = AsyncMongoDBTransport(
#             mongo_uri=mongo_uri, db="wconcept", collection="styles.fetch", today=today, transport=httpx.AsyncHTTPTransport(
#                 retries=3, proxy= proxy
#             )
#         )
#         self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
#         el = asyncio.get_event_loop()
#         styles = el.run_until_complete(self.async_execute(context))
#         return styles

#     async def async_execute(self, context):
#         tasks = []
#         for sort_type in self.sort_types :
#             tasks.append(self.async_task(sort_type))
#         results = await asyncio.gather(*tasks)
#         product_ids = list(reduce(lambda x, y: x + y, results, []))
#         return product_ids

#     async def async_task(self,sort_type):

#         first_page_data = await self.fetch_post(self.url,payload= {**self.base_payload, "page_idx":1,  "sort_type": sort_type})
#         soup = BeautifulSoup(first_page_data, 'html.parser')
#         total_span  = soup.select_one("div.category__product--title h3 span")
#         total_products = int(total_span.text.replace(",", "")) # type: ignore
#         total_page = total_products // self.product_num_per_page +1
#         total_page = min(total_page,self.max_page)
#         tasks = []
#         for page in range(1,total_page+1):
#             tasks.append(self.fetch(url=self.url, payload={**self.base_payload, "page_idx":page,  "sort_type": sort_type}))
#         responses = await asyncio.gather(*tasks)

#         return responses

#     async def fetch_post(self, url, payload:dict,retries:int=0, max_retries:int=5, **kwargs) :
#         if retries > max_retries:
#             self.log.error(f"{url} after {max_retries} retries")
#             raise kwargs.get("exception", Exception("Failed to fetch data"))
#         try:
#             async with self.semaphore:
#                 res = await self.aclient.post(url, data = payload)
#         except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
#             logging.error(f"{url}, {e}")

#             return await self.fetch(self.url, payload= payload ,retries=retries+1, exception=e)

#         assert res.is_success, f"Failed to fetch data from {url} :: {res.status_code} :: {res.text}"
#         return res.text
