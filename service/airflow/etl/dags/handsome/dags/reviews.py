from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from handsome.ops.fetch.reviews import FetchStyleIDFromDBOperator, FetchStyleReviewOperator
from handsome.ops.transform.review import TransformReviewResponseDataOperator
from handsome.ops.load.review import ReviewLoadDataOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig


__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"

dag = DAG(
    dag_id="handsome.reviews",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=7),
    default_args=__DEFAULT_ARGS__,
    tags=["handsome", "reviews"],
    catchup=False,
    concurrency=10,
)
with dag:
    ids = FetchStyleIDFromDBOperator(task_id="fetch.style.id", size=1000)
    fetch_reviews = FetchStyleReviewOperator.partial(
        task_id="fetch.styles.review",
        retries=10,
        max_active_tis_per_dag=5,
        cache_config=MongoDBCacheConfig(database="handsome", collection="reviews.fetch"),
        max_page=10,
    ).expand(style_id_list=ids.output)
    transforms = TransformReviewResponseDataOperator.partial(
        task_id="transform.data", retries=0, max_active_tis_per_dag=4
    ).expand(reviews=fetch_reviews.output)
    load = ReviewLoadDataOperator.partial(task_id="load", retries=0, max_active_tis_per_dag=5).expand(
        reviews=transforms.output
    )
    mv_update = TriggerDagRunOperator(
        task_id="update.mv.reviews",
        trigger_dag_id="refresh.materialviews",
        trigger_run_id=None,
        execution_date=pendulum.now("UTC"),
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        trigger_rule="all_done",
        conf={
            "refresh_tables": ["mv_style_total_review_count", "mv_style_latest_price", "mv_style_total_aspect_count"],
        },
    )
    ids >> fetch_reviews >> transforms >> load >> mv_update
