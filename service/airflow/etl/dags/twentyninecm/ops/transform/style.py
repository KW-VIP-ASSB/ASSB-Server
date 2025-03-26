import json
from typing import Any, List
from datetime import datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.models.variable import Variable
from twentyninecm.models import TwentyNineInfoParser, TwentynineStyleInfo
from twentyninecm.schema import TwentyninecmSchema

import logging
logger = logging.getLogger()
class TransformStyleInfoResponseDataOperator(BaseOperator):
    response_list: List


    def __init__(self, response_list: List[Any],ctg_gender_map:dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.response_list = response_list
        self.ctg_gender_map =ctg_gender_map
    def execute(self, context:Context):
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        logger.info(f"Processing style info of type {type(self.response_list[0])}: {self.response_list[0]}")

        transformed_data = [
            style for style_infos in self.response_list
            for style in (self.transform_data(style_infos, date=today) or [])
        ]
        self.log.debug(f"Result data Count : {len(transformed_data)}")
        return transformed_data

    def filter_gender(self, parsed_data):
        for item in parsed_data:
            gender_list = set()  # 중복 제거를 위한 set
            for category in item.categories:
                category_code = category["category1_code"]
                if category_code in self.ctg_gender_map:  # 매핑 테이블에 존재하는 경우
                    gender_list.add(self.ctg_gender_map[category_code])
            item.gender = list(gender_list)  # gender 필드 업데이트
        return parsed_data


    def transform_data(self, style_info:dict, date:datetime)->List[TwentynineStyleInfo]:
        logger.info(f"Processing style info of type {type(style_info)}: {style_info}")
        try:
            parsed_data = TwentyNineInfoParser.parse(style_info, date)
            parsed_data = self.filter_gender(parsed_data)
            result = map(dict,parsed_data)
            return result

        except KeyError as e:
            self.log.error(f"Missing key in product info: {style_info}. Error: {e}")
            raise e
        except Exception as e:
            self.log.error(f"Unexpected error processing product info: {style_info}. Error: {e}")
            raise e



class TransformStyleDataOperator(BaseOperator):
    styles: List[dict]
    def __init__(self, styles: List[dict], **kwargs) -> None:
        super().__init__(**kwargs)
        self.styles = styles

    def execute(self, context:Context):
        self.log.debug(f"Input data Count : {len(self.styles)}")
        today = context['execution_date'].in_timezone("UTC").today() # type: ignore
        for style in self.styles:
            parsed_datas = TwentyninecmSchema.build(style,today)
            results = [result.model_dump() for result in parsed_datas ]
        return results
