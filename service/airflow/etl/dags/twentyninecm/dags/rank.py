from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from core.infra.ops.reduce import ReduceListOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from twentyninecm.ops.fetch.styles import (
    FetchStyleIdListRankBycategories,
)
from twentyninecm.ops.load.rank import LoadDataOperator
from twentyninecm.ops.transform.style import TransformStyleDataOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig

__DEFAULT_ARGS__ = {
    "owner": "yslee",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 10


dag = DAG(
    dag_id="29cm.style.rank",
    start_date=pendulum.datetime(2024, 5, 1),
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["29cm", "styles"],
    catchup=False,
    concurrency=__CONCURRENCY__,
    max_active_runs=1,
)
with dag:
    inputs = Variable.get("29cm.rank.inputs", default_var={}, deserialize_json=True)
    
    categories =list(inputs.get("ctg_codes",[]))
    
    fetch_styles = FetchStyleIdListRankBycategories.partial(
        task_id="fetch.styles.rank",
        cache_config=MongoDBCacheConfig(database="29cm", collection="styles.fetch"),
        inputs = inputs,
        retries=10,
    ).expand(ctg_list=categories)
    
    reduce_styles = ReduceListOperator(
        task_id="fetch.styles.reduce",
        data=fetch_styles.output,
        n=30,
        retries=0,
    )
    
    transform_styles = TransformStyleDataOperator.partial(
        task_id="transform.styles",
    ).expand(styles=reduce_styles.output)
    
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

    fetch_styles >> reduce_styles  >> transform_styles >> load >> [mv_update, image_download]  # type: ignore
