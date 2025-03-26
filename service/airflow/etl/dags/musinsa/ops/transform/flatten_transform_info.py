from typing import Any, Dict, List

from airflow import XComArg
from airflow.models.baseoperator import BaseOperator


class FlattenTransformDataOperator(BaseOperator):
    def __init__(self, transform_data: XComArg, **kwargs) -> None:
        super().__init__(**kwargs)
        self.transform_data = transform_data

    def execute(self, context) -> List[Dict[str, Any]]:
        transform_data = self.transform_data.resolve(context)
        combined_data = []
        for data_list in transform_data:
            combined_data.extend(list(data_list))
        self.log.info(f"Combined {len(combined_data)} items from transform data")
        return combined_data
