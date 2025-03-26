import asyncio
import ssl
from datetime import timedelta

import httpx
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models.variable import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from core.entity.style import Style
from core.infra.database.models.connections import Database
from core.infra.httpx_cache.mongo import AsyncMongoDBTransport, MongoDBCacheConfig
from core.infra.utils.utils import DivideList
import sqlalchemy as sa

__DEFAULT_ARGS__ = {"owner": "yslee", "retries": 1, "retry_delay": timedelta(seconds=10)}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 2


with DAG(
    dag_id="29cm.styles.html",
    start_date=pendulum.datetime(2024, 5, 1),
    max_active_runs=1,
    default_args=__DEFAULT_ARGS__,
    tags=["29cm", "styles", "html"],
    catchup=False,
) as dag:
    # Fetching subcategories from Variable
    @task(task_id="get.styles.ids", retries=0)
    def get_style_ids(
        size=10000,
        site_id: str = "qSh_hnGm49bnPArh",
        db_conn_id="ncp-pg-db",
        **context,
    ) -> list[str]:
        style_ids = context["dag_run"].conf.get("style_ids", [])
        if len(style_ids) == 0:
            postgres_hook = PostgresHook(postgres_conn_id=db_conn_id)
            db = Database(db_hook=postgres_hook.connection, echo=False)
            with db.session() as session:
                stmt = sa.select(Style.style_id).where(Style.site_id == site_id)
                style_ids = session.execute(stmt).scalars().all()
        print(len(style_ids))
        return DivideList(size=size).deivide(style_ids)

    @task(task_id="fetch.styles.html", retries=10)
    def get_htmls(
        style_ids: list[str],
        cache_config: MongoDBCacheConfig,
        proxy_key: str = "smartproxy.kr",
        mongo_conn_id: str = "ncp-mongodb",
        timeout: int = 120,
        **context,
    ) -> list[str]:
        semaphore = asyncio.Semaphore(20)
        mongo_uri = MongoHook(mongo_conn_id=mongo_conn_id).uri
        proxy = Variable.get(proxy_key)
        httpx_cache_transport = AsyncMongoDBTransport(
            mongo_uri=mongo_uri,
            db=cache_config.database,
            collection=cache_config.collection,
            transport=httpx.AsyncHTTPTransport(retries=3, proxy=proxy),
        )
        aclient = httpx.AsyncClient(
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "User-Agent": (
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Version/16.6 Mobile/15E148 Safari/604.1"
                ),
            },
            timeout=timeout,
            transport=httpx_cache_transport,
        )

        async def fetch(url, retries: int = 0, max_retries: int = 5, **kwargs) -> str:
            if retries > max_retries:
                print(f"{url} after {max_retries} retries")
                raise kwargs.get("exception", Exception("Failed to fetch data"))
            try:
                async with semaphore:
                    res = await aclient.get(url)
            except (
                ssl.SSLError,
                httpx.RemoteProtocolError,
                httpx.ProxyError,
                httpx.TimeoutException,
                httpx.HTTPError,
                httpx.ConnectError,
                httpx.ConnectTimeout,
            ) as e:
                print(e)
                return await fetch(url, retries=retries + 1, exception=e)
            if res.status_code in [404]:
                return None
            assert res.is_success, f"{url} ::  {res.status_code} :: {res.text}"
            return res.text

        __URL_TEMPLATE__ = "https://product.29cm.co.kr/catalog/{id}"
        el = asyncio.get_event_loop()
        tasks = [fetch(__URL_TEMPLATE__.format(id=style_id)) for style_id in style_ids]
        el.run_until_complete(asyncio.gather(*tasks))

    style_ids = get_style_ids()
    htmls = get_htmls.partial(
        cache_config=MongoDBCacheConfig(database="29cm", collection="style.html"),
    ).expand(style_ids=style_ids)
