from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from core.infra.ops.reduce import ReduceStyleIdListOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from musinsa.ops.fetch.styles import (
    FetchStyleIdListRankBycategories,
    FetchStyleOperator,
)
from musinsa.ops.load.rank import LoadDataOperator
from musinsa.ops.transform.styles import TransformStyleDataOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10


dag = DAG(
    dag_id="musinsa.style.rank",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["musinsa", "styles"],
    catchup=False,
    concurrency=__CONCURRENCY__,
    max_active_runs=1,
)
with dag:
    urls = Variable.get("musinsa.rank.urls", default_var=[], deserialize_json=True)
    fetch_styles = FetchStyleIdListRankBycategories.partial(
        task_id="fetch.styles.by.categories.rank",
        cache_config=MongoDBCacheConfig(database="musinsa", collection="styles.fetch"),
        retries=10,
        max_page=3,
    ).expand(inputs=urls)
    reduce_styles = ReduceStyleIdListOperator(
        task_id="fetch.styles.reduce",
        data=fetch_styles.output,
        n=30,
        task_ids=fetch_styles.task_id,
        retries=0,
    )
    fetch_styles_info = FetchStyleOperator.partial(
        task_id="fetch.styles.info",
        cache_config=MongoDBCacheConfig(database="musinsa", collection="styles.info"),
        retries=10,
    ).expand(styles=reduce_styles.output)
    transform_styles = TransformStyleDataOperator.partial(
        task_id="transform.styles",
    ).expand(styles=fetch_styles_info.output)
    load = LoadDataOperator.partial(
        task_id="load.data",
        retries=0,
        max_active_tis_per_dag=5,
    ).expand(styles=transform_styles.output)

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
    image_download = TriggerDagRunOperator(
        task_id="dag.image.download",
        trigger_dag_id="images.processing",
        trigger_run_id=None,
        execution_date=pendulum.now("UTC"),
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
    )

    fetch_styles >> reduce_styles >> fetch_styles_info >> transform_styles >> load >> [mv_update, image_download]  # type: ignore
