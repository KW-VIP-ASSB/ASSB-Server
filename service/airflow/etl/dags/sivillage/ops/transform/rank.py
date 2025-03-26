# import json
# from typing import Any, List
# from datetime import datetime
# from airflow.models.baseoperator import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from airflow.utils.context import Context
# from airflow.dags.sivillage.model import SivillageStyleInfo , SivillageInfoParser


# class TransformRankResponseDataOperator(BaseOperator):
#     response_list: List[dict]
#     @apply_defaults
#     def __init__(self, response_list: List[Any], **kwargs) -> None:
#         super().__init__(**kwargs)
#         self.response_list = response_list

#     def execute(self, context:Context):
#         today = context['execution_date'].in_timezone("UTC").today() # type: ignore
#         transformed_data = []
#         self.log.debug(f"Input data Count : {len(self.response_list)}")
#         for style_info in self.response_list:
#             style = self.transform_data(style_info, date=today)
#             if style is not None:
#                 transformed_data.append(style.model_dump())
#         self.log.debug(f"Result data Count : {len(transformed_data)}")
#         return transformed_data

#     def transform_data(self, style_info:dict, date:datetime)-> SivillageStyleInfo|None:
#         self.log.debug(f"Processing style info of type {type(style_info)}: {style_info}")
#         try:
#             parsed_data = SivillageInfoParser.parse(style_info, date)
#             return parsed_data
#         except KeyError as e:
#             self.log.error(f"Missing key in product info: {style_info}. Error: {e}")
#             raise e
#         except Exception as e:
#             self.log.error(f"Unexpected error processing product info: {style_info}. Error: {e}")
#             raise e
