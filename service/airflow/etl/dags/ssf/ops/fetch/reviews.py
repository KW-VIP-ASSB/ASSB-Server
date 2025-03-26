
import asyncio
import logging
import ssl
import httpx
from airflow.models.baseoperator import BaseOperator
from airflow.models.variable import Variable
from airflow.utils.context import Context
from openai import max_retries
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.infra.database.models.connections import Database
from core.entity.style import Style
import sqlalchemy as sa
from core.infra.utils.utils import DivideList

from bs4 import BeautifulSoup
from datetime import datetime


class FetchStyleIDFromDBOperator(BaseOperator):
    site_id:str = "ssf"
    def __init__(
        self,
        *,
        db_conn_id="ncp-pg-db",
        n: int | None = None,
        size: int | None = None,
        limit: int|None= None,
        **kwargs,
        ):
        self.db_conn_id = db_conn_id
        self.divide_list = DivideList(n=n, size=size, is_shuffle=True)
        self.limit = limit
        super().__init__(**kwargs)

    def execute(self, context: Context):
        postgres_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        db = Database(db_hook=postgres_hook.connection, echo=False)
        with db.session() as session:
            stmt = sa.select(Style.style_id, Style.id).where(Style.brand_id==self.site_id)
            style_id_list = session.execute(stmt).all()
        ids = [dict(style_id=style_id, style_idx=style_idx) for style_idx, style_id in style_id_list]
        self.log.info(f"Style IDs: {len(ids)}")
        ids_divided = self.divide_list.deivide(ids)
        return ids_divided



class FetchStyleReviewOperator(BaseOperator):
    url_template = "https://www.ssfshop.com/public/goods/detail/listReview?pageNo={page_num}&sortFlag=&godNo={style_id}&selectAllYn=Y&godEvlTurn=0"
    base_header =  {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Connection" : "keep-alive",
        "Accept" :"*/*",
    }
    

    def __init__(
        self,
        *,
        style_ids: list[dict],
        max_page: int = 10,
        max_concurrency: int = 50,
        timeout: float = 320.0,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        max_pagenum = 5,
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.style_ids = style_ids
        self.max_page = max_page
        self.timeout = timeout
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.max_pagenum = max_pagenum
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config = cache_config
        super().__init__(**kwargs)

    def execute(self, context: Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        self.log.info(f"style_id count: {len(self.style_ids)}")
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)
        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri, db=self.cache_config.db, collection=self.cache_config.collection, today=today, transport=httpx.AsyncHTTPTransport(
                retries=3,
                proxy=proxy
            )
        )
        self.aclient= httpx.AsyncClient( timeout=self.timeout, transport=httpx_cache_transport, follow_redirects=True)
        el = asyncio.get_event_loop()
        reviews = el.run_until_complete(self.async_task())
        reviews = [self.transform(review, date=today) for review in reviews]
        return reviews


    async def async_task(self):
        reviews = []
        task = []
        for row in self.style_ids:
            task.append(self.async_task_fetch_all_reviews(row))
        results = await asyncio.gather(*task)
        for result in results:
            if result is None:
                continue
            reviews.extend(result)
        self.log.info(f"reviews count: {len(reviews)}")
        self.log.info(reviews[0])
        return reviews

    def get_total_review_count(self, html :str):
        soup = BeautifulSoup(html, "html.parser")
        if not soup.find("input", id = "reviewTotalRow") :
            return None
        review_div = soup.find("input", id = "reviewTotalRow")
        return (int(review_div["value"]) // 15) + 1# type: ignore

    async def async_task_fetch_all_reviews(self, row):
        style_idx = row["style_idx"]
        first_url = self.url_template.format(page_num = 1 , style_id = style_idx)
        tasks = []
        
        first_page=  await self.fetch_get(first_url)
        total_page = self.get_total_review_count(first_page)  # type: ignore

        if total_page is None :
            return None
        total_page = min(total_page, self.max_page)

        for pagenum in range(1,total_page):
            url = self.url_template.format(page_num = pagenum , style_id = style_idx)
            tasks.append(self.fetch_get(url))
        results = await asyncio.gather(*tasks)
        review_divs = [self.get_review_div(reivew_contents) for reivew_contents in results]
        reviews = []
        for result in review_divs:
            if result is None:
                continue
            review = dict()
            review['content'] = result
            review['style_idx'] = style_idx
            review['style_id'] = row["style_id"]
            reviews.append(review)
        return reviews
    
    def transform(self, review:dict, date:datetime)->list[dict]:
        content = review['content']
        style_id = review['style_id']
        soup = BeautifulSoup(content, 'html.parser')
        review_items = soup.find_all('li')

        reviews = []
        for item in review_items:
            review_id = item.get('id')
            rating_tag = item.find("div", class_ = "list-status")
            rating = len(rating_tag.select('i'))
            user_tag = item.find('span', class_='list-id')
            author_name = user_tag.text.strip() if user_tag else ""
            review_content_tag = item.find('p', class_='review-txts')
            text = review_content_tag.text.strip() if review_content_tag else ""
            review_date_tag = item.find('span', class_='list-date')
            review_date = review_date_tag.text.strip() if review_date_tag else ""
            review_date = str(datetime.strptime(review_date, "%Y.%m.%d")) 
            review = dict(
                style_review=dict(
                    style_id=style_id,
                    review_idx= review_id,
                    brand_id="ssf",
                ),

                review_id=review_id,
                brand_id="ssf",

                writed_at=review_date,
                writed_date=review_date,

                author_name=author_name,
                author_id=author_name,

                rating=rating,
                recommended=False, # recommend 정보 알수없음
                verifed_purchaser=True,
                title=None,
                text=text,
            )

            reviews.append(review)

        return reviews
    def get_review_div(self, html :str):
        soup = BeautifulSoup(html, "html.parser")
        review_div = soup.find("div", id="searchGoodsReviewList")
        
        if not review_div:
            return None
        
        return str(review_div) 
    
    async def fetch_get(self, url: str, **kwargs)->str|None:
        retries = kwargs.get("retries", 0)
        max_retries = kwargs.get("max_retries", 3)

        if retries > max_retries:
            return None

        self.aclient.headers = self.base_header
            

        try:
            async with self.semaphore:
                res = await self.aclient.get(url)

        except (ssl.SSLError,httpx.RemoteProtocolError,httpx.ProxyError)  as e:
            self.log.error(f"{e}: url: {url}")
            retries +=1
            return await self.fetch_get(url,retries=retries)

        if res.status_code in [429, 500, 502, 503, 504 ]:
            self.log.error(f"return None url: {url} :: {res.status_code} :: {res.text}")
            return None

        assert res.is_success, f"{url}: {res.status_code} :: {res.text}"
        return res.text
