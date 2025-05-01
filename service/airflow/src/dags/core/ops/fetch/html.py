import asyncio
import hashlib
import httpx
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
import ssl
from airflow.providers.mongo.hooks.mongo import MongoHook
import xmltodict,json
from core.infra.utils.utils import DivideList,parse_socks5_proxy
from airflow.operators.python import BaseOperator
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
import asyncio

class FetchStyleHtmlOperator(BaseOperator):
    headers = {
        "User-Agent": "PostmanRuntime/7.37.3",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8,ja;q=0.7,zh-CN;q=0.6,zh;q=0.5",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Sec-Ch-Ua-Platform": "macOS",
    }
    url: str
    category_id: int | str
    proxy_key: str

    def __init__(
        self,
        *,
        styles: list[dict],
        timeout: float = 120.0*1000,
        proxy_key: str= "smartproxy.us",
        semaphore:int = 2,
        mongo_conn_id: str = "ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.styles = styles
        self.timeout = timeout
        self.proxy_key = proxy_key
        self.semaphore = asyncio.Semaphore(semaphore)
        self.head_semaphore = asyncio.Semaphore(semaphore*10)
        self.mongo_conn_id = mongo_conn_id
        self.cache_config = cache_config

        super().__init__(**kwargs)

    def generate_cache_key(self,*, method:str="GET", url:str, date: datetime=None) -> str:
        key_data = { "method": method, "url": url, "date": date }
        if date is None:
            key_data.pop("date")
        return hashlib.sha256(json.dumps(key_data, sort_keys=True, default=str).encode()).hexdigest()

    def lookup(self, _id:str)-> dict|None:
        doc = self.mongo_hook.find(
            mongo_collection=self.cache_config.collection,
            mongo_db=self.cache_config.db,
            query={"_id":_id},
            find_one=True,
        )

        if doc is None:
            return None
        doc.pop("_id")
        return doc

    def execute(self, context: Context):
        proxy = Variable.get(self.proxy_key)
        self.aclient = httpx.AsyncClient(headers=self.headers,timeout=120,follow_redirects=True, transport=httpx.AsyncHTTPTransport(retries=5,proxy=proxy))
        self.proxy = parse_socks5_proxy(proxy)
        self.mongo_hook = MongoHook(mongo_conn_id=self.mongo_conn_id)
        el = asyncio.get_event_loop()
        tasks = [self.fetch(url=style["url"], metadata=style) for style in self.styles]
        htmls = el.run_until_complete(asyncio.gather(*tasks))
        htmls = [html for html in htmls if html is not None]
        return htmls

    async def fetch(self, url, retries=0,max_retries=5, **kwargs):
        if retries > max_retries:
            return None
        metadata = kwargs.get("metadata", {})
        key = self.generate_cache_key(method="GET", url=url)
        doc = self.lookup(key)
        if doc is not None:
            metadata['html'] = doc['html']
            return metadata

        try:
            async with self.head_semaphore:
                res = await self.aclient.head(url, timeout=self.timeout)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            self.log.error(f"{url}, {e}")
            return await self.fetch(url, retries=retries+1, exception=e)
        if res.status_code in [403,404]:
            return None

        try:
            async with self.semaphore:
                async with async_playwright() as p:
                    browser_type =  p.chromium
                    browser = await browser_type.launch(
                        headless=True,
                    )
                    context = await browser.new_context(proxy=self.proxy, java_script_enabled=True)
                    page = await context.new_page()
                    await stealth_async(page)
                    response = await page.goto(url, timeout=self.timeout, wait_until="load")

                    assert response is not None, f"{url} : {response}"
                    html = await page.content()
            if response.status >= 400:
                self.log.error(f"{url} :: {response.status}")
                return None
        except Exception as e:
            self.log.error(f"{url} :: {e}")
            return await self.fetch(url=url, retries=retries+1, max_retries=max_retries)

        assert len(html) > 100, f"{url} : {html}"

        if html:
            self.mongo_hook.insert_one(mongo_collection=self.cache_config.collection, mongo_db=self.cache_config.db, doc={
                "_id": key,
                "html": html,
                "metadata": metadata,
                "created_at": datetime.now(),
            })
        metadata['html'] = html
        return  metadata
