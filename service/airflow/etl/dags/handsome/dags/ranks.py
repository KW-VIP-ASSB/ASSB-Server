# from datetime import timedelta

# import pendulum
# from airflow import DAG
# from airflow.models.variable import Variable

# from handsome.ops.transform.flatten_transform_info import FlattenTransformDataOperator

# from handsome.ops.fetch.ranks import FetchStyleRankListOperator
# from handsome.ops.transform.rank import TransformStyleRankResponseDataOperator
# from handsome.ops.load.rank import HandsomeLoadRankOperator
# from handsome.ops.fetch.reviews import FetchStyleIDFromDBOperator, FetchStyleReviewOperator
# from handsome.ops.transform.review import TransformReviewResponseDataOperator
# from handsome.ops.load.review import ReviewLoadDataOperator

# from airflow.models.xcom_arg import XComArg

# __DEFAULT_ARGS__ = {
#     "owner": "yslee",
#     "retries": 1,
#     "retry_delay": timedelta(seconds=10),
# }
# __DATABASE_ID__ = "airflow"
# __CONCURRENCY__ = 2

# with DAG(
#     dag_id="handsome.ranks",
#     start_date=pendulum.datetime(2024, 5, 1),
#     max_active_runs=1,
#     schedule_interval=timedelta(days=3),
#     default_args=__DEFAULT_ARGS__,
#     tags=["handsome", "ranks"],
#     catchup=False,
#     concurrency=__CONCURRENCY__,
# ) as dag:
#     # Fetching subcategories from Variable
#     category_data = Variable.get(key="handsome.categories", default_var=[], deserialize_json=True)
#     ctg_gender_map = Variable.get("handsome.categories.dict", default_var={}, deserialize_json=True)
#     config = Variable.get("handsome.ranks.config", default_var={}, deserialize_json=True)
#     retries_kwargs = dict(retries=3, retry_delay=timedelta(seconds=1))

#     fetch_rank = FetchStyleRankListOperator.partial(task_id="fetch.style_ranks", **retries_kwargs).expand(
#         ctg_code=category_data
#     )
#     transform_response = TransformStyleRankResponseDataOperator.partial(
#         task_id="transform.response.data", retries=0
#     ).expand(response_list=fetch_rank.output)
#     concat_transform = FlattenTransformDataOperator(
#         task_id="transform.data.flatten", transform_data=transform_response.output, retries=0
#     )
#     transforms = transform_response >> concat_transform

#     load = HandsomeLoadRankOperator(
#         task_id="load.data",
#         transform_data=transforms.output,  # type: ignore
#         retries=0,
#     )
#     fetch_rank >> transforms >> load  # type: ignore
