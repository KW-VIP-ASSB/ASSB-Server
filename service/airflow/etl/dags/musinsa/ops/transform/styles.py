import json
from typing import Any, List
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
from musinsa.schema import MusinsaSchema


class TransformStyleDataOperator(BaseOperator):
    styles: List[dict]
    @apply_defaults
    def __init__(self, styles: List[dict], **kwargs) -> None:
        super().__init__(**kwargs)
        self.styles = styles

    def execute(self, context:Context):
        self.log.debug(f"Input data Count : {len(self.styles)}")
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        results = [ MusinsaSchema.build(style,today).model_dump() for style in self.styles]
        return results
