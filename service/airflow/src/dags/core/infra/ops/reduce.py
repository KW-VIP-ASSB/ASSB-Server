import logging
from typing import Any
from airflow import XComArg
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from core.infra.utils.utils import DivideList
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)

class ReduceStyleIdListOperator(BaseOperator):
    data: XComArg

    def __init__(
        self, *, data: XComArg, n: int | None = None, size: int | None = None, task_ids:str|None, key:str="return_value", **kwargs
    ):
        self.data = data
        self.n = n
        self.size = size
        self.task_ids=task_ids
        self.key=key
        self.divide_list = DivideList(n=n, size=size)
        super().__init__(**kwargs)

    def execute(self, context: Context, **kwargs):
        task_instance = context["task_instance"]
        outputs = task_instance.xcom_pull(task_ids=self.task_ids, key="return_value")
        product_ids = [goods_no for sublist in outputs for goods_no in sublist]
        self.log.info(f"count: {len(product_ids)}")
        product_ids_divided = self.divide_list.deivide(product_ids)
        return product_ids_divided

class ReduceListOperator(BaseOperator):
    data: list[list[Any]]

    def __init__(
        self, *, data: list[list[Any]], n: int | None = None, size: int | None = None,  **kwargs
    ):
        self.data = data
        self.n = n
        self.size = size
        self.divide_list = DivideList(n=n, size=size)
        super().__init__(**kwargs)

    def execute(self, context: Context, **kwargs):
        results = []
        ti = context['ti']
        xcom_data = ti.xcom_pull(task_ids=self.data.task_ids, key=self.data.key, include_prior_runs=True)
        for result in xcom_data:
            if result is None:
                continue
            results.extend(result)
        self.log.info(f"total count : {len(results)}")
        return self.divide_list.deivide(results)
