from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from core.infra.ops.reduce import ReduceStyleIdListOperator
from musinsa.ops.fetch.styles import FetchStyleIdListOperator, FetchStyleInfoOperator
from musinsa.ops.load.style import MusinsaLoadDataOperator
from musinsa.ops.transform.flatten_transform_info import FlattenTransformDataOperator
from musinsa.ops.transform.transform_response import TransformResponseDataOperator

from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "jsp",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10


dag = DAG(
    dag_id="musinsa.styles",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["musinsa", "styles"],
    catchup=False,
    concurrency=__CONCURRENCY__,
    max_active_runs=1,
)
with dag:
    retries_kwargs = dict(retries=3, retry_delay=timedelta(seconds=1))
    brands = Variable.get(key="musinsa.brands", default_var=[], deserialize_json=True)
    fetch_styles = FetchStyleIdListOperator.partial(
        task_id="fetch.styles",
        cache_config=MongoDBCacheConfig(database="musinsa", collection="styles.fetch"),
        **retries_kwargs,
    ).expand(brand_name=brands)
    reduce_styles = ReduceStyleIdListOperator(
        task_id="fetch.styles.reduce", data=fetch_styles.output, n=24, task_ids=fetch_styles.task_id
    )
    fetch_styles_info = FetchStyleInfoOperator.partial(
        task_id="fetch.styles.info",
        cache_config=MongoDBCacheConfig(database="musinsa", collection="styles.info"),
        **retries_kwargs,
    ).expand(product_id_list=reduce_styles.output)
    transform_response = TransformResponseDataOperator.partial(task_id="transform.response.data", retries=0).expand(
        response_list=fetch_styles_info.output
    )
    

    load = MusinsaLoadDataOperator.partial(
        task_id="load.data",
        retries=0,
        max_active_tis_per_dag=1,
    ).expand(transform_data=transform_response.output)  

    
    fetch_styles >> reduce_styles >> fetch_styles_info >> transform_response >> load 