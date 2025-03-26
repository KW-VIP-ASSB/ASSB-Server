from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from handsome.ops.fetch.styles import FetchStyleInfoListOperator
from handsome.ops.transform.style import TransformStyleInfoResponseDataOperator
from handsome.ops.load.style import HandsomeLoadInfoOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 2

dag = DAG(
    dag_id="handsome.styles",
    start_date=pendulum.datetime(2024, 5, 1),
    max_active_runs=1,
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["handsome", "styles"],
    catchup=False,
    concurrency=__CONCURRENCY__,
)
with dag:
    category_data = Variable.get(key="handsome.categories", default_var=[], deserialize_json=True)
    ctg_gender_map = Variable.get("handsome.categories.dict", default_var={}, deserialize_json=True)
    retries_kwargs = dict(retries=3, retry_delay=timedelta(seconds=1))

    fetch_styles = FetchStyleInfoListOperator.partial(
        task_id="fetch.styles",
        cache_config=MongoDBCacheConfig(database="handsome", collection="styles.fetch"),
        **retries_kwargs,
    ).expand(ctg_code=category_data)
    
    transform_response = TransformStyleInfoResponseDataOperator.partial(
        task_id="transform.response.data", ctg_gender_map=ctg_gender_map, retries=0
    ).expand(response_list=fetch_styles.output)
 
    load = HandsomeLoadInfoOperator.partial(
        task_id="load.data",
        retries=0,
    ).expand(transform_data=transform_response.output)  # type: ignore
    
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

    fetch_styles >>transform_response  >> load >> [mv_update, trigger_image_download]  # type: ignore
