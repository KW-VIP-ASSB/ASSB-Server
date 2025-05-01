
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
    site_id:str = "iylQhcSbkgVxi0Ye"
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
    url_template = "https://goods.musinsa.com/api2/review/v1/view/list?page={pagenum}&pageSize=30&goodsNo={style_idx}&sort=up_cnt_desc&selectedSimilarNo={style_idx}&myFilter=false&hasPhoto=false&isExperience=false"
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
        style_id_list: list[dict],
        timeout: float = 320.0,
        max_concurrency: int = 50,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        max_page: int = 10,
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.style_id_list = style_id_list
        self.timeout = timeout
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.max_page = max_page
        self.cache_config= cache_config
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
        url = self.url_template.format(pagenum=0, style_idx=style_idx)
        first_page_data = await self.fetch_get(url)
        total_pages = first_page_data.get("data", {}).get("page", {})
        if total_pages is None:
            return []
        total_pages = total_pages.get("totalPages", 0)
        total_pages = min(total_pages, self.max_page)
        tasks = []
        for pagenum in range(0, total_pages + 1):
            url = self.url_template.format(pagenum=pagenum, style_idx=style_idx)
            tasks.append(self.fetch_get(url))
        results = await asyncio.gather(*tasks, return_exceptions=False)
        reviews = []
        for result in results:
            data = result.get("data", {})
            for review in data.get("list", []):
                review['style_idx'] = style_idx
                review['style_id'] = row["style_id"]
            reviews.extend(data.get("list", []))
        return reviews

    async def fetch_get(self, url: str, **kwargs):
        try:
            async with self.semaphore:
                res = await self.aclient.get(url)
        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError)  as e:
            self.log.error(e)
            return await self.fetch_get(url)
        if res.status_code in [403]:
            return await self.fetch_get(url)

        assert res.is_success, f"{url}: {res.status_code}"
        return res.json()
