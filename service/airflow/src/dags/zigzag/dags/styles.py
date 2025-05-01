from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from core.infra.ops.reduce import ReduceStyleIdListOperator
from zigzag.ops.fetch.styles import FetchStyleIdInfoListOperator 
from zigzag.ops.load.style import StyleLoadDataOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "jsp",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10

dag = DAG(
    dag_id="zigzag.styles",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["zigzag", "styles"],
    catchup=False,
    concurrency=__CONCURRENCY__,
    max_active_runs=1,
)

with dag:
    retries_kwargs = dict(retries=3, retry_delay=timedelta(seconds=1))
    
    inputs = Variable.get(key="zigzag.inputs", default_var={
                "inputs":{
                    "2792":"상의->긴소매 티셔츠",
                    "2793":"니트/카디건->카라니트"
                },
                "retries":10,
                "size":500,
                "proxy_key":"smartproxy.kr",
                "cache_config":{
                    "database":"29cm",
                    "collection":"styles"
                },
                "max_active_tis_per_dag":{
                    "fetch":5,
                    "transform":2,
                    "load":5
                }
            }, deserialize_json=True)
    
    #TODO 로컬 테스트 완료 시 슬라이스 삭제
    fetch_inputs = list(inputs["inputs"].keys())
    
    fetch_styles = FetchStyleIdInfoListOperator.partial(
        task_id="fetch.styles",
        retries=inputs.get("retries", 10),  
        ctg_dict = inputs.get("inputs"),
        proxy_key=inputs.get("proxy_key", "smartproxy.kr"),
        max_active_tis_per_dag=inputs.get("max_active_tis_per_dag", {}).get("fetch", 5),
        cache_config=MongoDBCacheConfig(database="zigzag", collection="styles"),
    ).expand(ctg_code=fetch_inputs)
    
    reduce_styles = ReduceStyleIdListOperator(
        task_id="fetch.styles.reduce", data=fetch_styles.output, size=500, task_ids=fetch_styles.task_id
    )

    
    load = StyleLoadDataOperator.partial(task_id="load.data", max_active_tis_per_dag=1, retries=0).expand( styles=reduce_styles.output )

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
    ) 
    