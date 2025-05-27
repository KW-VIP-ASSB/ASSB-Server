
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



class FetchStyleDetailOperator(BaseOperator):
    url = "https://api.zigzag.kr/api/2/graphql/GetCatalogProductDetailPageOption"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, zstd",
        "Origin": "https://zigzag.kr",
        "Referer": "https://zigzag.kr/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Cookie": '_fwb=199fvbXrueq6uzLCpC1ewLC.1743476769698; _atrk_siteuid=VlJP7V_FaQY39Q7S; _ga=GA1.1.1695983964.1743476770; _fcOM={"k":"6f09dd5e8147f1d8-1bedf5421956a7a5324463c","i":"211.114.206.72.94065","r":1743476770525}; ZIGZAGUUID=2f6e3c99-dc30-4818-bbdb-5bce5cec5933.U6Beyxf2K1TCoSwfxxdt7h3%2BA%2B6apjMFYEaXgeZNAks; afUserId=785566ce-ad02-4aa5-8731-1b5f0ab4b6b9-p; ab180ClientId=3d5eda06-02c1-4abf-af58-cd113ae8810e; connect.sid=s%3A4LmAV6bVBK2gugolcUicdDYN-AS-r0rm.FosE5ZWSV8I1KTnR3yNaTOBa0fho51Y%2B8qihTLq4UJU; _cm_mmc=sa_google_pc; _atrk_ssid=s6RGw91FWr6C4fUQ2xu-yT; _gcl_gs=2.1.k1$i1744691499$u51079458; ab.storage.deviceId.103ec90c-cf52-4ad7-b6e9-12957ab2c361=g%3Ad861b434-3ead-7ae3-ebdd-801a09d4c3b9%7Ce%3Aundefined%7Cc%3A1743476770516%7Cl%3A1744691501921; AF_SYNC=1744691502259; _cm_cc=3; appier_utmz=%7B%22csr%22%3A%22(adwords%20gclid)%22%2C%22ccn%22%3A%22sa_01_pc_zigzag_zigzag%22%2C%22cmd%22%3A%22search%22%2C%22ctr%22%3A%22%25EC%25A7%2580%25EA%25B7%25B8%25EC%259E%25AC%25EA%25B7%25B8%22%2C%22cct%22%3A%22main%22%2C%22timestamp%22%3A1744693370%2C%22lcsr%22%3A%22(adwords%20gclid)%22%7D; _gcl_aw=GCL.1744693371.CjwKCAjw5PK_BhBBEiwAL7GTPV5hfC2_4VJS_hO0xq8uH7Y0LXSbBX7ZHYUBulMU8l8mWv_klV3OShoCI-sQAvD_BwE; ab.storage.sessionId.103ec90c-cf52-4ad7-b6e9-12957ab2c361=g%3Abb026210-f0ad-256f-c9c4-3b040a3de266%7Ce%3A1744696465566%7Cc%3A1744691501918%7Cl%3A1744694665566; _ga_3JHT92YZJ8=GS1.1.1744691500.3.1.1744694724.2.0.0; amp_b31370=DYeE3S8D7m4TNbEuE8NhJH...1iorrf5da.1ioruhi73.0.0.0; appier_pv_counterPageView_9e66=3; appier_page_isView_PageView_9e66=449cfbd7af938566a21b0a71b26d895cc63fd9ad5945a07219bc322b4ac3f7e8; appier_pv_counterViewTwoPages_1a5e=1; appier_page_isView_ViewTwoPages_1a5e=449cfbd7af938566a21b0a71b26d895cc63fd9ad5945a07219bc322b4ac3f7e8; _atrk_sessidx=9'
    }

    base_graphql_query = {"query":"query GetCatalogProductDetailPageOption($catalog_product_id: ID!, $input: PdpBaseInfoInput) { pdp_option_info(catalog_product_id: $catalog_product_id, input: $input) { catalog_product { id fulfillment_type shop_main_domain external_code minimum_order_quantity maximum_order_quantity coupon_available_status scheduled_sale_date promotion_info { bogo_required_quantity promotion_id promotion_type bogo_info { required_quantity discount_type discount_amount discount_rate_bp } } product_image_list { url origin_url pdp_thumbnail_url pdp_static_image_url image_type } product_option_list { id order name code required option_type value_list { id code value static_url jpeg_url } } matching_catalog_product_info { id name is_able_to_buy pdp_url fulfillment_type browsing_type external_code product_price { max_price_info { price color { normal } badge { text color { normal } } } coupon_discount_info { discount_amount } final_discount_info { discount_price } } discount_info { color title image_url order } shipping_fee { fee_type base_fee minimum_free_shipping_fee } option_list { id order name code required option_type value_list { id code value static_url jpeg_url } } item_list { id name price price_delta final_price item_code sales_status display_status remain_stock is_zigzin delivery_type expected_delivery_date expected_delivery_time discount_info { image_url title color order } item_attribute_list { id name value value_id } wms_notification_info { active } } } product_additional_option_list { id order name code required option_type value_list { id code value static_url jpeg_url } } item_list { id name price price_delta final_price item_code sales_status display_status remain_stock is_zigzin delivery_type expected_delivery_date expected_delivery_time discount_info { image_url title color order } item_attribute_list { id name value value_id } wms_notification_info { active } } additional_item_list { id name price price_delta item_code sales_status display_status option_type is_zigzin delivery_type expected_delivery_date item_attribute_list { id name value value_id } wms_notification_info { active } } custom_input_option_list { name is_required: required max_length } color_image_list { is_main image_url image_width image_height webp_image_url color_list } } } }",
                            "variables":{}}
    def __init__(
        self,
        *,
        style_ids: list[dict],
        max_count: int | None = 5,
        max_concurrency: int = 10,
        timeout: float = 320.0,
        proxy_key:str = "smartproxy.kr",
        mongo_conn_id:str ="ncp-mongodb",
        max_pagenum = 5,
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.style_ids = style_ids
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
                                            proxy = proxy, 
                                            retries=3),
        )
        
        self.aclient = httpx.AsyncClient(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport, follow_redirects=True)
        
        el = asyncio.get_event_loop()
        details = el.run_until_complete(self.async_task())
        
        transformed_data = [
            self.transform_data(detail, date=today) for detail in details if self.transform_data(detail, date=today) is not None
        ]
        return transformed_data
    
    def transform_data(self, detail: dict, date) -> dict:
        content = detail.get("content")
        style_id = detail.get("style_idx")
        site_id = "vPu2SsvYkCYXDCiz"

        try:
            data = content.get("data")
            if data is None:
                return None
            original_data = data.get("pdp_option_info", {}).get("catalog_product", {})

            product_image_list = [img["url"] for img in original_data.get("product_image_list", [])]

            # 옵션 처리
            options = {}
            for option in original_data.get("product_option_list", []):
                option_key = option["name"].strip()
                if option_key not in options:
                    options[option_key] = []

                for value in option["value_list"]:
                    options[option_key].append({
                        "code": value.get("code"),
                        "value": value.get("value")
                    })

            # 이미지 -> HTML
            def transform_image_list_to_html(product_image_list: list[str]) -> str:
                return "\n".join(
                    f'<img src="{url}" style="margin:5px;" loading="lazy" />'
                    for url in product_image_list
                )

            description = transform_image_list_to_html(product_image_list)

            # 옵션 키 -> 표준화된 facet 키로 매핑
            facet_key_map = {
                "컬러": "color", "색상": "color", "COLOR": "color",
                "사이즈": "size", "SIZE": "size", "사이즈1": "size",
                "타입": "type", "TYPE": "type"
            }

            facets = []
            style_facets = []
            extracted_values = {}

            for raw_key, values in options.items():
                standard_key = facet_key_map.get(raw_key)
                if not standard_key:
                    continue  # 알 수 없는 옵션은 무시하거나 log

                extracted_values[standard_key] = [v["value"] for v in values]

                # facet, style_facet 생성
                for value in extracted_values[standard_key]:
                    facets.append({
                        "name": value,
                        "type": standard_key,
                        "version": "original"
                    })
                    style_facets.append({
                        "site_id": site_id,
                        "facet_idx": value,
                        "style_idx": style_id,
                        "facet_type": standard_key
                    })

            transformed_result = {
                "product_image_list": product_image_list,
                "options": options
            }

            return {
                "facets": facets,
                "style_facets": style_facets,
                "style_metadata": {
                    "site_id": site_id,
                    "style_idx": style_id,
                    "data": transformed_result,
                    "description": description
                },
                "style_id": style_id,
                "description": description,
                **extracted_values  # color, size, type 등 각 리스트 포함
            }

        except KeyError as e:
            self.log.error(f"Missing key in product info: {detail['style_id']}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {detail['style_id']}. Error: {e}")
            raise e

    async def async_task(self):
        task = []
        for row in self.style_ids:
            task.append(self.async_task_fetch_detail(row))
            
        results = await asyncio.gather(*task)
        
        
        return results
    async def async_task_fetch_detail(self, row):
        style_idx = row["style_idx"]
        page = await self.fetch(style_idx)
        if page is None :
            return {}
        detail = {
            'content': page ,
            'style_idx': style_idx,
            'style_id': row["style_id"]
        }
        
        return detail

    async def fetch(self, style_id: str |None, retries: int = 0, max_retries: int = 5) -> dict:
        if retries > max_retries:
            self.log.error(f"Page {style_id} failed after {max_retries} retries")
            return {}

        payload = self.base_graphql_query.copy()
        payload["variables"].update({"catalog_product_id":style_id,"input":{"catalog_product_id":style_id,"entry_source_type":""}})

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

        assert res.is_success, f"{self.url} ::: {res.status_code} ::: {payload}"
        return res.json()