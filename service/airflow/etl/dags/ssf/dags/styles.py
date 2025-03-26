from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from core.infra.ops.reduce import ReduceStyleIdListOperator
from ssf.ops.fetch.styles import  FetchStyleOperator
from ssf.ops.load.style import StyleLoadDataOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10

dag = DAG(
    dag_id="ssf.styles",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["ssf", "styles"],
    catchup=False,
    concurrency=__CONCURRENCY__,
    max_active_runs=1,
)

with dag:
    retries_kwargs = dict(retries=3, retry_delay=timedelta(seconds=1))
    sub_category_data = Variable.get(key="ssf.categories", default_var=[ "SFMA41A36", "SFMA42A04"], deserialize_json=True)
    ctg_gender_map = Variable.get(key="ssf.categories.gender.map", default_var={
  "SFMA41": "여성",
  "SFMA41A07": "여성",
  "SFMA41A21": "여성",
  "SFMA41A03": "여성",
  "SFMA41A02": "여성",
  "SFMA41A01": "여성",
  "SFMA41A06": "여성",
  "SFMA41A04": "여성",
  "SFMA41A05": "여성",
  "SFMA41A31": "여성",
  "SFMA41A37": "여성",
  "SFMA41A12": "여성",
  "SFMA41A36": "여성",
  "SFMA42": "남성",
  "SFMA42A05": "남성",
  "SFMA42A06": "남성",
  "SFMA42A04": "남성",
  "SFMA42A19": "남성",
  "SFMA42A02": "남성",
  "SFMA42A03": "남성",
  "SFMA42A01": "남성",
  "SFMA42A11": "남성",
  "SFMA42A07": "남성",
  "SFMA42A36": "남성",
  "SFMA42A37": "남성",
  "SFME35": "골프",
  "SFME35A01": "골프여성",
  "SFME35A08": "골프여성",
  "SFME35A07": "골프남성",
  "SFME35A09": "골프남성"
}, deserialize_json=True)

    fetch_styles = FetchStyleOperator.partial(
        task_id="fetch.styles",
        retries=10,
        ctg_gender_map =  ctg_gender_map, 
        max_active_tis_per_dag=5,
        cache_config=MongoDBCacheConfig(database="ssf", collection="styles"),
    ).expand(ctg_code=sub_category_data)

    reduce_styles = ReduceStyleIdListOperator(
        task_id="fetch.styles.reduce", data=fetch_styles.output, size=500, task_ids=fetch_styles.task_id
    )

    load = StyleLoadDataOperator.partial(task_id="load.data",max_active_tis_per_dag=1, retries=0).expand(styles=reduce_styles.output)

    mv_update = TriggerDagRunOperator(
        task_id="update.mv.reviews",
        trigger_dag_id="refresh.materialviews",
        trigger_run_id=None,
        execution_date=pendulum.now("UTC"),
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        conf={
            "refresh_tables": ["main.mv_style_total_review_count"],
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
