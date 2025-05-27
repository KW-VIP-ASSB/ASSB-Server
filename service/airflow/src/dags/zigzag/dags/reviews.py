from datetime import timedelta

import pendulum
from airflow import DAG
from zigzag.ops.load.review import ReviewLoadDataOperator
from zigzag.ops.fetch.reviews import FetchStyleReviewOperator, FetchStyleIDFromDBOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig
from core.infra.ops.reduce import ReduceStyleIdListOperator


__DEFAULT_ARGS__ = {
    "owner": "jsp",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10


dag = DAG(
    dag_id="zigzag.reviews",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=7),
    default_args=__DEFAULT_ARGS__,
    tags=["zigzag", "reviews"],
    catchup=False,
    concurrency=10,
)


with dag:
    ids = FetchStyleIDFromDBOperator(task_id="fetch.style.id", size=1000)

    fetch_reviews = FetchStyleReviewOperator.partial(
        task_id="fetch.styles.review",
        retries=10,
        max_active_tis_per_dag=5,
        cache_config=MongoDBCacheConfig(database="zigzag", collection="review"),
    ).expand(style_ids=ids.output)

    
    reduce_styles = ReduceStyleIdListOperator(
        task_id="fetch.styles.reduce", data=fetch_reviews.output, n=24, task_ids=fetch_reviews.task_id
    )

    load = ReviewLoadDataOperator.partial(task_id="load.data",max_active_tis_per_dag=1, retries=0).expand( reviews=reduce_styles.output)

   
    ids >> fetch_reviews >> reduce_styles >> load 
