from datetime import timedelta

import pendulum
from airflow import DAG
from sivillage.ops.load.review import ReviewLoadDataOperator
from sivillage.ops.fetch.reviews import FetchStyleReviewOperator, FetchStyleIDFromDBOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig
from core.infra.ops.reduce import ReduceStyleIdListOperator


__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10


dag = DAG(
    dag_id="sivillage.reviews",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=7),
    default_args=__DEFAULT_ARGS__,
    tags=["sivillage", "reviews"],
    catchup=False,
    concurrency=10,
)


with dag:
    ids = FetchStyleIDFromDBOperator(task_id="fetch.style.id", size=1000)

    fetch_reviews = FetchStyleReviewOperator.partial(
        task_id="fetch.styles.review",
        retries=10,
        max_active_tis_per_dag=5,
        cache_config=MongoDBCacheConfig(database="sivillage", collection="reviews"),
    ).expand(style_ids=ids.output)

    
    reduce_styles = ReduceStyleIdListOperator(
        task_id="fetch.styles.reduce", data=fetch_reviews.output, n=24, task_ids=fetch_reviews.task_id
    )

    load = ReviewLoadDataOperator.partial(task_id="load.data",max_active_tis_per_dag=1, retries=0).expand( reviews=reduce_styles.output)

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

    ids >> fetch_reviews >> reduce_styles >> load >> mv_update
