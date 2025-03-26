import json
from typing import Any, List
from datetime import datetime
from functools import partial

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from handsome.rank_schema import HandsomeRankParser


class TransformStyleRankResponseDataOperator(BaseOperator):
    response_list: List

    def __init__(self, response_list: List[Any], **kwargs) -> None:
        super().__init__(**kwargs)
        self.response_list = response_list


    def execute(self, context:Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        self.log.info(f"Processing style info of type {type(self.response_list[0])}: {self.response_list[0]}")
        # TODO 시간 차이 크게 나는데 이유 확인해보기
        transformed_data = [
            style for style_infos in self.response_list
            for style in (self.transform_data(style_infos, date=today) or [])
        ]
        # transformed_data = []
        # transformed_func=partial(self.transform_data, date = today)
        # for style_infos in self.response_list:
        #     transformed = map(transformed_func, style_infos)
        #     transformed_data.extend(transformed)
        self.log.debug(f"Result data Count : {len(transformed_data)}")
        return transformed_data



    def transform_data(self, style_info:dict, date:datetime)-> list:
        self.log.info(f"Processing style info of type {type(style_info)}: {style_info}")
        if isinstance(style_info, str):
            self.log.error(style_info, TypeError("str"))
            return []
        try:

            parsed_data = HandsomeRankParser.parse(style_info, date)

            return parsed_data

        except KeyError as e:
            self.log.error(f"Missing key in product info: {style_info}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {style_info}. Error: {e}")
            raise e
