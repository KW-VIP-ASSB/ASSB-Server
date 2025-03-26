import asyncio
import logging
from typing import List
import ssl
import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook



class FetchStyleIdInfoListOperator(BaseOperator):
    url_template = "https://recommend-api.29cm.co.kr/api/v4/best/items?categoryList={ctg_code}&periodSort={period}&limit=100&offset=0"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/16.6 Mobile/15E148 Safari/604.1"
        ),
    }

    def __init__(
        self,
        *,
        categories: str,
        period:List[str]=["ONE_WEEK","ONE_DAY","ONE_MONTH","NOW"],
        timeout: float = 120.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.categories = categories
        self.periods = period
        self.timeout = timeout
        self.max_concurrency = max_concurrency  # 동시 실행 개수 제한
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config = cache_config
        super().__init__(**kwargs)

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)
        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri, db=self.cache_config.db, collection=self.cache_config.collection, today=today, transport=httpx.AsyncHTTPTransport(
                proxy=proxy, retries=3,
            )
        )
        self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        el = asyncio.get_event_loop()
        styles = el.run_until_complete(self.async_task(context))
        return styles

    async def async_task(self, context : Context):
        tasks = []
        for period in self.periods:
            url = self.url_template.format(ctg_code=self.categories, period=period)
            tasks.append(self.fetch(url))
        results = await asyncio.gather(*tasks)
        return results


    async def fetch(self, url, retries:int=0, max_retries:int=5, **kwargs) -> dict:
        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(f"{url}, {e}")
            return await self.fetch(url, retries=retries+1, exception=e)

        assert res.is_success, f"Failed to fetch data from {url}  {res.status_code} {res.text}"
        return res.json()



class FetchStyleIdListRankBycategories(FetchStyleIdInfoListOperator):
    
    def __init__(
        self,
        *,
        inputs = dict,
        ctg_list = str,
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.ctg_code = ctg_list  
        self.periods = inputs["periods"] # type: ignore
        self.timeout = timeout
        self.max_concurrency = max_concurrency  
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config=  cache_config
        super(FetchStyleIdInfoListOperator,self).__init__(**kwargs)

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        styles = super().execute(context=context)
        for style in styles:
            style['araas_date'] = today
        return styles

    
