from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from core.infra.ops.reduce import ReduceStyleIdListOperator
from sivillage.ops.fetch.styles import FetchStyleInfoOperator, FetchStyleOperator
from sivillage.ops.load.style import StyleLoadDataOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10

dag = DAG(
    dag_id="sivillage.styles",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["sivillage", "styles"],
    catchup=False,
    concurrency=__CONCURRENCY__,
    max_active_runs=1,
)

with dag:
    retries_kwargs = dict(retries=3, retry_delay=timedelta(seconds=1))
    sub_category_data = Variable.get(key="sivillage.categories", default_var={}, deserialize_json=True)

    fetch_styles = FetchStyleOperator.partial(
        task_id="fetch.styles",
        retries=10,
        max_active_tis_per_dag=5,
        cache_config=MongoDBCacheConfig(database="sivillage", collection="styles"),
    ).expand(ctg_code=sub_category_data)
    
    reduce_styles = ReduceStyleIdListOperator(
        task_id="fetch.styles.reduce", data=fetch_styles.output, size=500, task_ids=fetch_styles.task_id
    )

    fetch_styles_info = FetchStyleInfoOperator.partial(
        task_id="fetch.styles.info",
        retries=10,
        max_active_tis_per_dag=5,
        cache_config=MongoDBCacheConfig(database="sivillage", collection="styles.info"),
    ).expand(styles=reduce_styles.output)

    
    load = StyleLoadDataOperator.partial(task_id="load.data", max_active_tis_per_dag=1, retries=0).expand( styles=fetch_styles_info.output )

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

    trigger_image_download = TriggerDagRunOperator(
        task_id="dag.task.image.download",
        trigger_dag_id="images.processing",
        trigger_run_id=None,
        execution_date=pendulum.now("UTC"),
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
    )

    (
        fetch_styles
        >> reduce_styles
        >> load
        >> [mv_update, trigger_image_download]
    )  # type: ignore
