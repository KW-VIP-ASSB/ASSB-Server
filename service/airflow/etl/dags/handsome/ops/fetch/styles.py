import asyncio
import json
import httpx
import ssl

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook
import xmltodict



class FetchStyleInfoListOperator(BaseOperator):
    # sortGbn=20 판매순

    url_template = (
        "https://www.thehandsome.com/api/display/1/ko/category/categoryGoodsList?dispMediaCd=10&sortGbn=10&pageSize={page_size}&pageNo={page_num}&norOutletGbCd=J&dispCtgNo={ctg_code}&productListLayout=4&theditedYn=N&sDispCateCode=&mDispCateCode=&lDispCateCode=&mainCategory={ctg_code}"
    )
    headers = {
        "Accept": "application/json, text/plain, */*",
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
        ctg_code: str,
        page_size: int = 21,
        timeout: float = 120.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.ctg_code = ctg_code
        self.page_size = page_size
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
                proxy=proxy, retries=10,
            )
        )
        self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        el = asyncio.get_event_loop()
        styles = el.run_until_complete(self.async_task(context))
        return styles

    async def async_task(self, context : Context):
        tasks = []
        url =self.url_template.format(
            ctg_code=self.ctg_code, page_size=self.page_size, page_num=1
        )
        first_page_data = await self.fetch( url=url)

        total_count = first_page_data.get("payload", {}).get("totCnt", 0)
        max_page_num = (int(total_count) // int(self.page_size))

        self.log.info(f"Total Items: {total_count}, Max Pages: {max_page_num}")

        for page_num in range(1,max_page_num +1):
            url =self.url_template.format(
            ctg_code=self.ctg_code, page_size=self.page_size, page_num=page_num
        )
            tasks.append(self.fetch( url=url))
        response_list = []
        results = await asyncio.gather(*tasks)
        for result in results:
            response_list.extend(self.extract_product_info(result))

        return response_list
    def parse_xml_to_json(self,response_text: str, url: str) -> dict:

        response_text = response_text.strip()
        try:
            json_data = dict(xmltodict.parse(response_text))  # XML -> JSON 변환
        except (json.JSONDecodeError, Exception) as e :
            self.log.info(f"Successfully parsed XML response from {url}, contents: {response_text}")
            raise e
        return json_data.get("Response", "")

    def extract_product_info(self, json_data):
        if not isinstance(json_data, dict):
            self.log.error(f"Error fetching data: {json_data}")
            raise TypeError("str")
        payload = json_data.get("payload", {})
        goods_list = payload.get("goodsList", [])
        main_ctg = payload.get("categoryDetailListRequest").get("mainCategory")
        parsed_products = []
        # gender 필드를 채우기 위한 main_ctg 추가

        try :
            for good in goods_list:
                added_good = {**good, "main_ctg" :main_ctg}
                parsed_products.append(added_good)
        except TypeError as e :
            self.log.error(f"{e} good :{good} - try unwrapping")
            goods_list = goods_list.get("goodsList",[])
            for good in goods_list:
                added_good = {**good, "main_ctg" :main_ctg}
                parsed_products.append(added_good)
        except:
            raise
        return parsed_products

    async def fetch(self, url, max_retries : int =3, retries :int = 0,**kwargs) -> dict:
        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            self.log.error(f"{url}, {e}")
            return await self.fetch(url, retries=retries+1, exception=e)

        if res.status_code in [403, 503]:
            self.log.error(f"{url}, {res.status_code} contents:{res.text[:100]}")
            return await self.fetch(url, retries=retries+1)

        assert res.is_success, f"{url}: {res.status_code}\n contents : {res.text[:100]}"

        if "xml" in res.headers["content-type"]:
            return self.parse_xml_to_json(res.text, url)  # XML을 JSON으로 변환
        try:
            return res.json()
        except json.JSONDecodeError as e:
            return self.parse_xml_to_json(res.text, url)
