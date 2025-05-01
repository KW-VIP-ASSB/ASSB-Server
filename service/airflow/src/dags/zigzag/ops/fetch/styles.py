import asyncio
import logging
import ssl
from typing import List
from datetime import datetime
import httpx
from airflow import XComArg
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from core.infra.httpx_cache.mongo import MongoDBTransport,MongoDBCacheConfig
from airflow.providers.mongo.hooks.mongo import MongoHook
from bs4 import BeautifulSoup
from zigzag.schema import ZigzagSchema 

class FetchStyleIdInfoListOperator(BaseOperator):
    url = "https://api.zigzag.kr/api/2/graphql/GetSearchResult"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, zstd",
        "Origin": "https://zigzag.kr",
        "Referer": "https://zigzag.kr/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Cookie": '_fwb=199fvbXrueq6uzLCpC1ewLC.1743476769698; _atrk_siteuid=VlJP7V_FaQY39Q7S; _ga=GA1.1.1695983964.1743476770; _fcOM={"k":"6f09dd5e8147f1d8-1bedf5421956a7a5324463c","i":"211.114.206.72.94065","r":1743476770525}; ZIGZAGUUID=2f6e3c99-dc30-4818-bbdb-5bce5cec5933.U6Beyxf2K1TCoSwfxxdt7h3%2BA%2B6apjMFYEaXgeZNAks; afUserId=785566ce-ad02-4aa5-8731-1b5f0ab4b6b9-p; ab180ClientId=3d5eda06-02c1-4abf-af58-cd113ae8810e; connect.sid=s%3A4LmAV6bVBK2gugolcUicdDYN-AS-r0rm.FosE5ZWSV8I1KTnR3yNaTOBa0fho51Y%2B8qihTLq4UJU; _cm_mmc=sa_google_pc; _atrk_ssid=s6RGw91FWr6C4fUQ2xu-yT; _gcl_gs=2.1.k1$i1744691499$u51079458; ab.storage.deviceId.103ec90c-cf52-4ad7-b6e9-12957ab2c361=g%3Ad861b434-3ead-7ae3-ebdd-801a09d4c3b9%7Ce%3Aundefined%7Cc%3A1743476770516%7Cl%3A1744691501921; AF_SYNC=1744691502259; _cm_cc=3; appier_utmz=%7B%22csr%22%3A%22(adwords%20gclid)%22%2C%22ccn%22%3A%22sa_01_pc_zigzag_zigzag%22%2C%22cmd%22%3A%22search%22%2C%22ctr%22%3A%22%25EC%25A7%2580%25EA%25B7%25B8%25EC%259E%25AC%25EA%25B7%25B8%22%2C%22cct%22%3A%22main%22%2C%22timestamp%22%3A1744693370%2C%22lcsr%22%3A%22(adwords%20gclid)%22%7D; _gcl_aw=GCL.1744693371.CjwKCAjw5PK_BhBBEiwAL7GTPV5hfC2_4VJS_hO0xq8uH7Y0LXSbBX7ZHYUBulMU8l8mWv_klV3OShoCI-sQAvD_BwE; ab.storage.sessionId.103ec90c-cf52-4ad7-b6e9-12957ab2c361=g%3Abb026210-f0ad-256f-c9c4-3b040a3de266%7Ce%3A1744696465566%7Cc%3A1744691501918%7Cl%3A1744694665566; _ga_3JHT92YZJ8=GS1.1.1744691500.3.1.1744694724.2.0.0; amp_b31370=DYeE3S8D7m4TNbEuE8NhJH...1iorrf5da.1ioruhi73.0.0.0; appier_pv_counterPageView_9e66=3; appier_page_isView_PageView_9e66=449cfbd7af938566a21b0a71b26d895cc63fd9ad5945a07219bc322b4ac3f7e8; appier_pv_counterViewTwoPages_1a5e=1; appier_page_isView_ViewTwoPages_1a5e=449cfbd7af938566a21b0a71b26d895cc63fd9ad5945a07219bc322b4ac3f7e8; _atrk_sessidx=9'
    }

    base_graphql_query = {"query":"fragment DefaultInput on SearchFilterValue { key type value attribute } fragment RangeInput on SearchFilterValue { key type attribute min_value { ...DecimalUnitNumber } max_value { ...DecimalUnitNumber } } fragment DecimalUnitNumber on DecimalUnitNumber { display_unit decimal is_unit_prefix number_without_decimal unit } fragment UxGoodsCardItemPart on UxGoodsCardItem { browsing_type position type image_url webp_image_url jpeg_image_url video_url log ubl { server_log } image_ratio aid uuid product_url shop_id shop_product_no shop_name title discount_rate discount_info { image_url title color } column_count catalog_product_id goods_id one_day_delivery { ...UxCommonText } has_coupon is_zpay_discount price final_price final_price_with_currency { currency decimal price_without_decimal display_currency is_currency_prefix } max_price max_price_with_currency { currency decimal price_without_decimal display_currency is_currency_prefix } is_zonly is_brand free_shipping zpay ranking sellable_status is_ad is_exclusive similar_search review_score display_review_count badge_list { image_url dark_image_url small_image_url small_dark_image_url } thumbnail_nudge_badge_list { image_url dark_image_url small_image_url small_dark_image_url } thumbnail_emblem_badge_list { image_url dark_image_url small_image_url small_dark_image_url } brand_name_badge_list { image_url dark_image_url small_image_url small_dark_image_url } managed_category_list { id category_id value key depth } is_plp_v2 } fragment UxCommonText on UxCommonText { text color { normal } html { normal } } fragment UxText on UxText { type text style is_html_text } fragment UxButton on UxButton { type text is_html_text style log link_url } fragment UxSearchedShop on UxSearchedShop { id name bookmark_count category category_list { doc_count key } alias_list status is_disabled is_zpay style_list typical_image_url is_saved_shop main_domain department_badge { size text text_color font_weight background_color background_opacity border { color style width radius } } } query GetSearchResult($input: SearchResultInput!) { search_result(input: $input) { end_cursor has_next filter_list { name collapse component_list { key name type ... on SearchChipButtonFilterComponent { value_list { count selected value image_url input { ...DefaultInput } } } ... on SearchBreadcrumbFilterComponent { value_list { id name order path input { ...DefaultInput } } } ... on SearchListFilterComponent { value_list { count id name path selected input { ...DefaultInput } } } ... on SearchRangeSliderFilterComponent { input { ...RangeInput } interval { ...DecimalUnitNumber } max_without_decimal min_without_decimal selected_max_without_decimal selected_min_without_decimal } ... on SearchMessageFilterComponent { text } } } ui_item_list { __typename ... on UxSearchResultHeader { type position total_count } ... on UxNoResults { type position no_results_main_title { type text is_html_text style } no_results_sub_title { type text is_html_text style } image_url aspect_ratio } ... on UxImageBannerGroup { type id aspect_ratio is_auto_rolling update_interval item_list { id image_url landing_url log } } ... on UxFullWidthImageBannerGroup { type id aspect_ratio is_auto_rolling update_interval item_list { id image_url landing_url log } } ... on UxTextTitle { type text sub_text info { title desc } } ...UxGoodsCardItemPart ... on UxGoodsGroup { type group_type id main_title { ...UxText } sub_title { ...UxText } image { type image_url aspect_ratio log link_url } more_button { ...UxButton } action_button { ...UxButton } goods_carousel { type style line_count item_column_count more_button { ...UxButton } component_list { ...UxGoodsCardItemPart } } is_ad } ... on UxShopRankingCardItem { type position shop_id ranking_shop_name: shop_name shop_typical_image_url ranking is_saved_shop variance { type value color } component_list { ...UxGoodsCardItemPart } action_button { ...UxButton } } ... on UxGoodsCarousel { type style line_count item_column_count more_button { ...UxButton } component_list { ...UxGoodsCardItemPart } } ... on UxLineWithMargin { type color height margin { top left bottom right } } ... on UxCheckButtonAndSorting { type check_button_item_list { str_id name selected image_url disabled html_name { normal } } sorting_item_list { str_id name selected description } zigzin_filter_item_list { str_id name selected html_name { normal } } } ... on UxTextAndMoreButton { type position total_count main_title { ...UxText } more_button { ...UxButton } } ... on UxSearchedShopCarousel { type position searched_shop_list { ...UxSearchedShop } } ... on UxGoodsFilterList { type filter_list { key name selected selected_count } } } } }",
                            "variables":{}}
    def __init__(
        self,
        *,
        ctg_dict: dict,
        ctg_code :str,
        max_pages: int = 2,
        timeout: float = 120.0,
        max_concurrency: int = 50,
        proxy_key: str = "smartproxy.kr",
        mongo_conn_id: str = "ncp-mongodb",
        cache_config: MongoDBCacheConfig,
        **kwargs,
    ):
        self.ctg_dict = ctg_dict
        self.ctg_code = ctg_code
        self.timeout = timeout
        self.max_pages = max_pages
        self.max_concurrency = max_concurrency  # 동시 실행 개수 제한
        self.proxy_key = proxy_key
        self.mongo_conn_id = mongo_conn_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.cache_config = cache_config
        super().__init__(**kwargs)

    def execute(self, context: Context):
        today = context["execution_date"].in_timezone("UTC").today()  # type: ignore
        mongo_uri = MongoHook(mongo_conn_id=self.mongo_conn_id).uri
        proxy = Variable.get(self.proxy_key)

        httpx_cache_transport = MongoDBTransport(
            mongo_uri=mongo_uri,
            db=self.cache_config.db,
            collection=self.cache_config.collection,
            transport=httpx.HTTPTransport( 
                                            # proxy = proxy, 
                                            retries=3),
        )
        
        self.client = httpx.Client(headers=self.headers, timeout=self.timeout, transport=httpx_cache_transport)
        
        styles = self.sync_task()
        transformed_data = []
        for item_list in styles :
            items = self.transform_data(item_list,date=today )
            transformed_data.extend(items)
        return transformed_data
    
    def transform_data(self, item_list:List, date)->List[ZigzagSchema]:
        try:
            parsed_datas = ZigzagSchema.build(item_list, date)
            
            return [parsed_data.model_dump() for parsed_data in parsed_datas]

        except KeyError as e:
            self.log.error(f"Missing key in product info: {item_list[0]}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {item_list[0]}. Error: {e}")
            raise e
        
    def sync_task(self):
        has_next = True
        results = []
        first_page = self.fetch( None)
        next_page, has_next, result = self.get_next_pages(first_page)
        results.append(result)
        page_num = 1
        while has_next and page_num < self.max_pages:
            page = self.fetch(next_page)
            if page is None:
                continue
            next_page, has_next, result = self.get_next_pages(page)
            results.append(result)
            page_num += 1
            
        return results
    
    def get_next_pages(self, data) -> int:
        search_result = data.get("data").get("search_result")
        if search_result:
            next_page = search_result["end_cursor"]
            has_next = search_result["has_next"]
            result =  search_result["ui_item_list"]
        assert search_result ,"search result is None"
        return next_page, has_next, result

    def fetch(self, after: str |None, retries: int = 0, max_retries: int = 5) -> dict:
        if retries > max_retries:
            self.log.error(f"Page {after} failed after {max_retries} retries")
            return {}

        payload = self.base_graphql_query.copy()
        payload["variables"].update({
            "input": {
                "display_category_id_list": [self.ctg_code],
                "page_id": "web_srp_clp_category",
                "after": after
            }
        })

        try:
            res = self.client.post(self.url, json=payload)
        except (ssl.SSLError, httpx.RemoteProtocolError, httpx.ProxyError,
                httpx.TimeoutException, httpx.ConnectError, httpx.ConnectTimeout) as e:
            logging.error(f"{self.url}: {e}:{payload}")
            return self.fetch(after, retries=retries + 1)

        if res.status_code in [400, 403]:
            self.log.error(f"{self.url} ::: {res.status_code} ::: {res.text} ::: {payload}")
            return self.fetch(after, retries=retries + 1)

        assert res.is_success, f"{self.url}::: {res.status_code} :::{payload}"
        return res.json()