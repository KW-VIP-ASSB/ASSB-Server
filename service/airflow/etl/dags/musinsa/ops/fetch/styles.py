import asyncio
import logging
import ssl
import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook



class FetchStyleIdListOperator(BaseOperator):
    url_template = "https://api.musinsa.com/api2/dp/v1/plp/goods?brand={brand_name}&gf=A&sortCode=POPULAR&page={page_num}&size=30&caller=BRAND"
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
        brand_name: str,
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.brand_name = brand_name
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
            mongo_uri=mongo_uri, db=self.cache_config.database, collection=self.cache_config.collection, transport=httpx.AsyncHTTPTransport(
                retries=3,proxy=proxy
            ), today=today
        )
        self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        el = asyncio.get_event_loop()
        styles = el.run_until_complete(self.async_task())
        return styles

    async def async_task(self):
        tasks = []
        url = self.url_template.format(brand_name=self.brand_name, page_num=1)
        first_page_data = await self.fetch(url)
        total_pages =  first_page_data.get("data", {}).get("pagination", {}).get("totalPages", 1)

        for page_num in range(1, total_pages + 1):
            url = self.url_template.format(brand_name=self.brand_name, page_num=page_num)
            tasks.append(self.fetch(url))

        results = await asyncio.gather(*tasks)
        styles = []
        for res in results:
            styles.extend(self.extract_product_info(res))
        return styles


    def extract_product_info(self, json_data):
        if not isinstance(json_data, dict):
            self.log.error(f"Error fetching data: {json_data}")
            return []
        if json_data:
            item_list = json_data.get("data", {}).get("list", [])
            item_list = json_data['data']
            item_list = item_list['list']
            return [item.get("goodsNo") for item in item_list if "goodsNo" in item]
        else:
            self.log.error("json_data is null")
            return []


    async def fetch(self, url,retries=0,max_retries=10, **kwargs) -> dict:
        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(e)
            return await self.fetch(url, retries=retries+1, exception=e)

        if res.status_code in [403]:
            self.log.error(f"{url} : {res.status_code} : {res.text}")
            return await self.fetch(url, retries=retries+1)

        assert res.is_success, f"{url} : {res.status_code} : {res.text}"
        return res.json()


class FetchStyleIdListBycategories(FetchStyleIdListOperator):
    def __init__(
        self,
        *,
        inputs: dict,
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.url_template = inputs['url']
        self.name = inputs['name']
        self.timeout = timeout
        self.max_concurrency = max_concurrency  # 동시 실행 개수 제한
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config=  cache_config
        super(FetchStyleIdListOperator,self).__init__(**kwargs)

    async def async_task(self):
        tasks = []
        url = self.url_template.format(page=1)
        first_page_data = await self.fetch(url)
        total_pages =  first_page_data.get("data", {}).get("pagination", {}).get("totalPages", 1)
        for page in range(1, total_pages + 1):
            url = self.url_template.format(page=page)
            tasks.append(self.fetch(url))
        results = await asyncio.gather(*tasks)
        styles = []
        for res in results:
            styles.extend(self.extract_product_info(res))
        return styles


class FetchStyleIdListRankBycategories(FetchStyleIdListBycategories):
    def __init__(
        self,
        *,
        inputs: dict,
        max_page:int,
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.inputs = inputs
        self.url_template = inputs['url']
        self.name = inputs['name']
        self.timeout = timeout
        self.max_page = max_page
        self.max_concurrency = max_concurrency  # 동시 실행 개수 제한
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config=  cache_config
        super(FetchStyleIdListOperator,self).__init__(**kwargs)

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        styles = super().execute(context=context)
        for rank, style in enumerate(styles):
            style['araas_rank'] = rank + 1
            style['araas_category'] = self.inputs['tagName']
            style['araas_date'] = today
            style['araas_rank_inputs']= self.inputs
        return styles

    async def async_task(self):
        tasks = []
        url = self.url_template.format(page=1)
        first_page_data = await self.fetch(url)
        total_pages =  first_page_data.get("data", {}).get("pagination", {}).get("totalPages", 1)
        total_pages = min(total_pages, self.max_page)
        for page in range(1, total_pages + 1):
            url = self.url_template.format(page=page)
            tasks.append(self.fetch(url))
        results = await asyncio.gather(*tasks)
        styles = []
        for res in results:
            styles.extend(res['data']['list'])
        return styles

class FetchStyleInfoOperator(BaseOperator):
    url_template = "https://goods-detail.musinsa.com/api2/goods/{product_id}"
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
        product_id_list: list[str],
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.product_id_list = product_id_list
        self.timeout = timeout
        self.max_concurrency = max_concurrency
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config = cache_config
        super().__init__(**kwargs)

    def execute(self, context: Context):
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)
        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri, db=self.cache_config.database, collection=self.cache_config.collection,
            transport=httpx.AsyncHTTPTransport(retries=3,proxy=proxy),
        )
        self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        response_list = asyncio.run(self.async_task())
        return response_list

    async def async_task(self):
        tasks = []
        for product_id in self.product_id_list:
            url = self.url_template.format(product_id=product_id)
            tasks.append(self.fetch(url))
        results = await asyncio.gather(*tasks)
        return results


    async def fetch(self, url,retries=0,max_retries=10, **kwargs) -> str|dict:
        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(f"{url}, {e}")
            return await self.fetch(url, retries=retries+1, exception=e)

        if res.status_code in [403]:
            self.log.error(f"{url} : {res.status_code} : {res.text}")
            return await self.fetch(url, retries=retries+1)

        assert res.is_success, f"{url}: {res.status_code}"
        return res.json()

class FetchStyleOperator(BaseOperator):
    url_template = "https://goods-detail.musinsa.com/api2/goods/{product_id}"
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
        styles: list[dict],
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
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)
        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri, db=self.cache_config.database, collection=self.cache_config.collection,
            transport=httpx.AsyncHTTPTransport(retries=3,proxy=proxy),
        )
        self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        response_list = asyncio.run(self.async_task())
        return response_list

    async def async_task(self):
        tasks = []
        for style in self.styles:
            url = self.url_template.format(product_id=style['goodsNo'])
            tasks.append(self.fetch(url))
        results = await asyncio.gather(*tasks)
        for result , style in zip(results, self.styles):
            style['araas_style_info'] = result['data']
        return self.styles


    async def fetch(self, url,retries=0,max_retries=10, **kwargs) -> str|dict:
        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(f"{url}, {e}")
            return await self.fetch(url, retries=retries+1, exception=e)

        if res.status_code in [403]:
            self.log.error(f"{url} : {res.status_code} : {res.text}")
            return await self.fetch(url, retries=retries+1)

        assert res.is_success, f"{url}: {res.status_code}"
        return res.json()
