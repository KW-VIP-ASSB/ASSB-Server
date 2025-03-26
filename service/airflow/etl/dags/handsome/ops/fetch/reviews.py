
import asyncio
import logging
import ssl
import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.variable import Variable
from airflow.utils.context import Context
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.infra.database.models.connections import Database
from core.entity.style import Style
import sqlalchemy as sa
from core.infra.utils.utils import DivideList



class FetchStyleIDFromDBOperator(BaseOperator):
    site_id:str = "iK9Z0qwLb-snTbnE"
    def __init__(self, db_conn_id="ncp-pg-db", n: int | None = None, size: int | None = None,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.divide_list = DivideList(n=n, size=size, is_shuffle=True)

    def execute(self, context: Context):
        postgres_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        db = Database(db_hook=postgres_hook.connection, echo=False)
        with db.session() as session:
            stmt = sa.select(Style.style_id, Style.id).where(Style.site_id==self.site_id)
            style_id_list = session.execute(stmt).all()
        ids = [dict(style_id=style_id, style_idx=style_idx) for style_idx, style_id in style_id_list]
        self.log.info(f"Style IDs: {len(ids)}")
        ids_divided = self.divide_list.deivide(ids)
        return ids_divided



class FetchStyleReviewOperator(BaseOperator):
    url_template = (
        "https://www.thehandsome.com/api/goods/1/ko/goods/{style_idx}/reviews?sortTypeCd=latest&revGbCd=&pageSize={page_size}&pageNo={page_num}&goodsClorNm=&goodsSzNm="
    )
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
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
        style_id_list: list[str],
        page_size: int = 10,
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        max_page: int = 10,
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.style_id_list = style_id_list
        self.page_size = page_size
        self.timeout = timeout
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.max_page = max_page
        self.cache_config = cache_config
        super().__init__(**kwargs)

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        self.log.info(f"style_id count: {len(self.style_id_list)}")
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)
        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri, db=self.cache_config.db, collection=self.cache_config.collection, today=today, transport=httpx.AsyncHTTPTransport(
                retries=3,proxy=proxy
            )
        )
        self.aclient= httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        el = asyncio.get_event_loop()
        reviews = el.run_until_complete(self.async_task())
        return reviews


    async def async_task(self):
        reviews = []
        task = []
        for row in self.style_id_list:
            task.append(self.async_task_fetch_all_reviews(row))
        results = await asyncio.gather(*task)
        for result in results:
            reviews.extend(result)
        self.log.info(f"reviews count: {len(reviews)}")
        self.log.info(reviews[0])
        return reviews

    async def async_task_fetch_all_reviews(self, row):
        style_idx = row["style_idx"]
        url = self.url_template.format(page_num=1, style_idx=style_idx, page_size = self.page_size)
        first_page_data = await self.fetch(url)
        total_reviews = first_page_data.get("payload", {}).get("revAllListTotalCnt", 0)
        total_pages = (total_reviews // self.page_size) + 1

        if total_pages == 0 :
            return []
        total_pages = min(total_pages, self.max_page)

        tasks = []
        for page_num in range(1, total_pages + 1):
            url = self.url_template.format(page_num=page_num, style_idx=style_idx, page_size = self.page_size)
            tasks.append(self.fetch(url))
        results = await asyncio.gather(*tasks, return_exceptions=False)
        reviews = []
        for result in results:
            data = result.get("payload", {})
            for review in data.get("revAllList", []):
                review['style_idx'] = style_idx
                review['style_id'] = row["style_id"]
            reviews.extend(data.get("revAllList", []))
        return reviews

    async def fetch(self, url: str,max_retries : int =3, retries :int = 0,**kwargs):
        if retries > max_retries:
            self.log.error(f"{url} after {max_retries} retries")
            raise kwargs.get("exception", Exception("Failed to fetch data"))
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError, httpx.TimeoutException, httpx.HTTPError, httpx.ConnectError, httpx.ConnectTimeout)  as e:
            logging.error(f"{url}, {e}")
            return await self.fetch(url, retries=retries+1, exception=e)

        assert res.is_success, f"Failed to fetch data from {url}: {res.status_code} \n contents : {res.text if hasattr(res, "text") else "None"}"
        return res.json()
