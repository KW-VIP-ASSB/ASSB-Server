from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from twentyninecm.ops.fetch.styles import FetchStyleIdInfoListOperator
from twentyninecm.ops.transform.style import TransformStyleInfoResponseDataOperator
from twentyninecm.ops.load.style import StyleLoadInfoOperator
import random
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from core.infra.httpx_cache.mongo import MongoDBCacheConfig


__DEFAULT_ARGS__ = {"owner": "yslee", "retries": 1, "retry_delay": timedelta(seconds=10)}
__DATABASE_ID__ = "airflow"
__CONCURRENCY__ = 2


with DAG(
    dag_id="29cm.styles",
    start_date=pendulum.datetime(2024, 5, 1),
    max_active_runs=1,
    schedule_interval=timedelta(days=3),
    default_args=__DEFAULT_ARGS__,
    tags=["29cm", "styles"],
    catchup=False,
) as dag:
    # Fetching subcategories from Variable
    categories = Variable.get(key="29cm.categories", default_var=[
            268103100, 268102100, 268106100, 268105100, 268104100,
            268107100, 268110100, 268117100, 268108100, 268109100,
            268116100, 268115100, 268114100, 272103100, 272102100,
            272104100, 272110100, 272112100, 272113100, 272105100,
            272111100, 272109100, 272114100, 268100100, 272100100,
            ], deserialize_json=True)  # fmt: off
    random.shuffle(categories)

    inputs = Variable.get(key="29cm.styles.inputs", default_var={"fetch": 5}, deserialize_json=True)
    ctg_gender_map = Variable.get(
        "29cm.categories.gender.map",
        default_var={
            "268103100": "여성의류", "268102100": "여성의류", "268106100": "여성의류", "268105100": "여성의류",
            "268104100": "여성의류", "268107100": "여성의류", "268110100": "여성의류", "268117100": "여성의류",
            "268108100": "여성의류", "268109100": "여성의류", "268116100": "여성의류", "268115100": "여성의류",
            "268114100": "여성의류", "268100100": "여성의류", "272103100": "남성의류", "272102100": "남성의류",
            "272104100": "남성의류", "272110100": "남성의류", "272112100": "남성의류", "272113100": "남성의류",
            "272105100": "남성의류", "272111100": "남성의류", "272109100": "남성의류", "272114100": "남성의류",
            "272100100": "남성의류",
        },
        deserialize_json=True,
    )  # fmt: off



    fetch_styles = FetchStyleIdInfoListOperator.partial(
        task_id="fetch.styles",
        retries=10,
        cache_config=MongoDBCacheConfig(database="29cm", collection="styles.fetch"),
        max_active_tis_per_dag=inputs.get("fetch", 5),
    ).expand(categories=categories)

    transform_response = TransformStyleInfoResponseDataOperator.partial(
        task_id="transform.response.data",
        retries=0,
        ctg_gender_map=ctg_gender_map,
    ).expand(response_list=fetch_styles.output)


    load = StyleLoadInfoOperator.partial(task_id="load.data", max_active_tis_per_dag =1, retries=0).expand(transform_data=transform_response.output)

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
        task_id="dag.image.download",
        trigger_dag_id="images.processing",
        trigger_run_id=None,
        execution_date=pendulum.now("UTC"),
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
    )
    fetch_styles >> transform_response >> load >> [mv_update, trigger_image_download]  # type: ignore
