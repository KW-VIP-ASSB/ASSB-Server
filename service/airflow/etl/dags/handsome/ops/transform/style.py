import json
from typing import Any, List
from datetime import datetime
from functools import partial

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from handsome.schema import HandsomeStyleInfo,HandsomeInfoParser

class TransformStyleInfoResponseDataOperator(BaseOperator):
    response_list: List


    def __init__(self,ctg_gender_map:dict, response_list: List[Any], **kwargs) -> None:
        super().__init__(**kwargs)
        self.response_list = response_list
        self.ctg_gender_map =ctg_gender_map
    def execute(self, context:Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        self.log.info(f"Processing style info of type {type(self.response_list[0])}: {self.response_list[0]}")
        # TODO 시간 차이 크게 나는데 이유 확인해보기
        transformed_data = [
            style for style_infos in self.response_list for style in (self.transform_data(style_infos, date=today) or [])
        ]
        return transformed_data

    def filter_gender(self, parsed_data: HandsomeStyleInfo)-> dict:
        self.log.info(f"input : {parsed_data}")
        main_ctg = parsed_data.main_ctg
        assert self.ctg_gender_map.get(main_ctg,""), f"cannot be found gender"
        parsed_data.gender = self.ctg_gender_map.get(main_ctg,"").get("gender")
        return dict(parsed_data)


    def transform_data(self, style_info:dict, date:datetime)-> list[dict]:
        self.log.info(f"Processing style info of type {type(style_info)}: {style_info}")
        if isinstance(style_info, str):
            self.log.error(style_info, TypeError("str"))
            return []
        try:

            parsed_data = HandsomeInfoParser.parse(style_info, date)
            transformed = map(self.filter_gender, parsed_data)

            return list(transformed)

        except KeyError as e:
            self.log.error(f"Missing key in product info: {style_info}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {style_info}. Error: {e}")
            raise e
