from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from twentyninecm.ops.fetch.reviews import FetchStyleIDFromDBOperator, FetchStyleReviewOperator
from twentyninecm.ops.load.review import ReviewLoadDataOperator
from twentyninecm.ops.transform.review import TransformReviewResponseDataOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


dag = DAG(
    dag_id="29cm.reviews",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=7),
    default_args=__DEFAULT_ARGS__,
    tags=["29cm", "reviews"],
    catchup=False,
    max_active_runs=1,
)


with dag:
    inputs = Variable.get(key="29cm.reviews.config", default_var={}, deserialize_json=True)

    max_active_tis_per_dag = inputs.get("max_active_tis_per_dag", {})

    ids = FetchStyleIDFromDBOperator(
        task_id="fetch.style.id",
        size=inputs.get("size", 1000),
    )
    fetch_reviews = FetchStyleReviewOperator.partial(
        task_id="fetch.review",
        retries=10,
        max_page=inputs.get("max_page", None),
        cache_config=MongoDBCacheConfig(database="29cm", collection="reviews.fetch"),
        max_active_tis_per_dag=max_active_tis_per_dag.get("fetch", 5),
    ).expand(style_id_list=ids.output)
    transforms = TransformReviewResponseDataOperator.partial(
        task_id="transform.data",
        retries=0,
        max_active_tis_per_dag=max_active_tis_per_dag.get("transform", 5),
    ).expand(reviews=fetch_reviews.output)
    load = ReviewLoadDataOperator.partial(
        task_id="load",
        retries=0,
        max_active_tis_per_dag=max_active_tis_per_dag.get("load", 1),
    ).expand(reviews=transforms.output)
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
