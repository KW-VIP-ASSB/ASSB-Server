
import asyncio
import logging
import ssl
from typing import List
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
from datetime import datetime,timezone
import pytz

class FetchStyleIDFromDBOperator(BaseOperator):
    site_id:str = "vPu2SsvYkCYXDCiz"
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
            stmt = sa.select(Style.style_id, Style.id).where(Style.site_id==self.site_id)
            style_id_list = session.execute(stmt).all()
        ids = [dict(style_id=style_id, style_idx=style_idx) for style_idx, style_id in style_id_list]
        self.log.info(f"Style IDs: {len(ids)}")
        ids_divided = self.divide_list.deivide(ids)
        return ids_divided



class FetchStyleReviewOperator(BaseOperator):
    url = "https://api.zigzag.kr/api/2/graphql/GetProductReviewList"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, zstd",
        "Origin": "https://zigzag.kr",
        "Referer": "https://zigzag.kr/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Cookie": '_fwb=199fvbXrueq6uzLCpC1ewLC.1743476769698; _atrk_siteuid=VlJP7V_FaQY39Q7S; _ga=GA1.1.1695983964.1743476770; _fcOM={"k":"6f09dd5e8147f1d8-1bedf5421956a7a5324463c","i":"211.114.206.72.94065","r":1743476770525}; ZIGZAGUUID=2f6e3c99-dc30-4818-bbdb-5bce5cec5933.U6Beyxf2K1TCoSwfxxdt7h3%2BA%2B6apjMFYEaXgeZNAks; afUserId=785566ce-ad02-4aa5-8731-1b5f0ab4b6b9-p; ab180ClientId=3d5eda06-02c1-4abf-af58-cd113ae8810e; connect.sid=s%3A4LmAV6bVBK2gugolcUicdDYN-AS-r0rm.FosE5ZWSV8I1KTnR3yNaTOBa0fho51Y%2B8qihTLq4UJU; _cm_mmc=sa_google_pc; _atrk_ssid=s6RGw91FWr6C4fUQ2xu-yT; _gcl_gs=2.1.k1$i1744691499$u51079458; ab.storage.deviceId.103ec90c-cf52-4ad7-b6e9-12957ab2c361=g%3Ad861b434-3ead-7ae3-ebdd-801a09d4c3b9%7Ce%3Aundefined%7Cc%3A1743476770516%7Cl%3A1744691501921; AF_SYNC=1744691502259; _cm_cc=3; appier_utmz=%7B%22csr%22%3A%22(adwords%20gclid)%22%2C%22ccn%22%3A%22sa_01_pc_zigzag_zigzag%22%2C%22cmd%22%3A%22search%22%2C%22ctr%22%3A%22%25EC%25A7%2580%25EA%25B7%25B8%25EC%259E%25AC%25EA%25B7%25B8%22%2C%22cct%22%3A%22main%22%2C%22timestamp%22%3A1744693370%2C%22lcsr%22%3A%22(adwords%20gclid)%22%7D; _gcl_aw=GCL.1744693371.CjwKCAjw5PK_BhBBEiwAL7GTPV5hfC2_4VJS_hO0xq8uH7Y0LXSbBX7ZHYUBulMU8l8mWv_klV3OShoCI-sQAvD_BwE; ab.storage.sessionId.103ec90c-cf52-4ad7-b6e9-12957ab2c361=g%3Abb026210-f0ad-256f-c9c4-3b040a3de266%7Ce%3A1744696465566%7Cc%3A1744691501918%7Cl%3A1744694665566; _ga_3JHT92YZJ8=GS1.1.1744691500.3.1.1744694724.2.0.0; amp_b31370=DYeE3S8D7m4TNbEuE8NhJH...1iorrf5da.1ioruhi73.0.0.0; appier_pv_counterPageView_9e66=3; appier_page_isView_PageView_9e66=449cfbd7af938566a21b0a71b26d895cc63fd9ad5945a07219bc322b4ac3f7e8; appier_pv_counterViewTwoPages_1a5e=1; appier_page_isView_ViewTwoPages_1a5e=449cfbd7af938566a21b0a71b26d895cc63fd9ad5945a07219bc322b4ac3f7e8; _atrk_sessidx=9'
    }

    base_graphql_query = {"query":"fragment ReviewListItem on UxReview { id product_id order_item_number status contents date_created date_updated rating fulfillment_badge { type text image_url image_size { width height } } country site_id type like_list { type count } reply_list { contents } user_reply_count requested_user { is_mine liked_list is_abuse_reported } attachment_list { original_url thumbnail_url status } additional_description attribute_list { question { label value category } answer { label value } } reviewer { body_text profile { id nickname masked_email profile_image_url is_ranker is_visible badge { ... on UxIconTextBadgeComponent { text { text } } } } } product_info { image_url name option_detail_list { name value } } seller_event_reward_info { badge { text { text } } } } query GetProductReviewList( $product_id: ID $limit_count: Int $skip_count: Int $has_attachment: Boolean $order: UxReviewListOrderType ) { ux_review_list( input: { product_id: $product_id has_attachment: $has_attachment order: $order pagination: { limit_count: $limit_count, skip_count: $skip_count } } ) { total_count item_list { ...ReviewListItem } } }",
                            "variables":{}}
    def __init__(
        self,
        *,
        style_ids: list[dict],
        max_count: int | None = 5,
        max_concurrency: int = 50,
        timeout: float = 320.0,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        max_pagenum = 5,
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.style_ids = style_ids[:10] #TODO TEST이후 제거
        self.max_count = max_count
        self.timeout = timeout
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.max_pagenum = max_pagenum
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config = cache_config
        super().__init__(**kwargs)


    def execute(self, context: Context):
        today = context["execution_date"].in_timezone("UTC").today()  # type: ignore
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)

        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri,
            db=self.cache_config.db,
            collection=self.cache_config.collection,
            transport=httpx.AsyncHTTPTransport( 
                                            # proxy = proxy, 
                                            retries=3),
        )
        
        self.aclient = httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        
        el = asyncio.get_event_loop()
        reviews = el.run_until_complete(self.async_task())
        
        transformed_data = []
        for item_list in reviews :
            items = self.transform_data(item_list,date=today)
            transformed_data.extend(items)
        return transformed_data
    
    def transform_data(self, item_list:dict, date)->List[dict]:
        parsed_reviews = []
        content = item_list.get("content")
        style_id = item_list.get("style_id")

        try:
            data = content.get("data")
            reviews = data.get("ux_review_list").get("item_list")
            if not reviews :
                return []
            for item in reviews:
                writed_at = datetime.fromtimestamp(item["date_created"] / 1000)
                parsed = dict(
                style_review=dict(
                    style_id=style_id,
                    review_idx=item["id"],
                    site_id="vPu2SsvYkCYXDCiz",
                    rating=item["rating"],
                    writed_date=writed_at.replace(hour=0, minute=0, second=0, microsecond=0),
                ),

                review_id=item["id"],
                site_id="vPu2SsvYkCYXDCiz",

                writed_at=writed_at,
                writed_date=writed_at.replace(hour=0, minute=0, second=0, microsecond=0),

                author_name=item["reviewer"]["profile"]["nickname"],
                author_id=item["reviewer"]["profile"]["id"],

                rating=int(item["rating"]),
                recommended=True,
                verifed_purchaser=True,
                title=None,
                text=item["contents"]
            )
            parsed_reviews.append(parsed)
            return parsed_reviews

        except KeyError as e:
            self.log.error(f"Missing key in product info: {item_list["style_id"]}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {item_list["style_id"]}. Error: {e}")
            raise e
        
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
        
        return reviews
    async def async_task_fetch_all_reviews(self, row):
        style_idx = row["style_idx"]
        reviews = []
        page = await self.fetch(style_idx)
        if page is None :
            return []
        review = {
            'content': page ,
            'style_idx': style_idx,
            'style_id': row["style_id"]
        }
        reviews.append(review)
        
        return reviews

    async def fetch(self, style_id: str |None, retries: int = 0, max_retries: int = 5) -> dict:
        if retries > max_retries:
            self.log.error(f"Page {style_id} failed after {max_retries} retries")
            return {}

        payload = self.base_graphql_query.copy()
        payload["variables"].update({"order":"BEST_SCORE_DESC","limit_count":self.max_count,"product_id":style_id})

        try:
            async with self.semaphore:
                res =await self.aclient.post(self.url, json=payload)
        except (ssl.SSLError, httpx.RemoteProtocolError, httpx.ProxyError,
                httpx.TimeoutException, httpx.ConnectError, httpx.ConnectTimeout) as e:
            logging.error(f"{self.url}: {e}:{payload}")
            return await self.fetch(style_id, retries=retries + 1)
        
        if res.status_code in [400, 403]:
            self.log.error(f"{self.url} ::: {res.status_code} ::: {res.text} ::: {payload}")
            return await self.fetch(style_id, retries=retries + 1)

        assert res.is_success, f"{self.url}::: {res.status_code} :::{payload}"
        return res.json()