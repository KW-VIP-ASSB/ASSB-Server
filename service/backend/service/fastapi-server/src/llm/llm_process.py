from src.llm.chains import review_chain
from src.llm.chains import get_fit_chain
from langchain_core import exceptions
from langsmith import traceable
import logging

logger = logging.getLogger(__name__)

@traceable
async def summarize_review(user_info, review_text):
    try:
        return review_chain.invoke({"user_info": user_info, "review_text": review_text})
    except exceptions.LangChainException as e:
        logger.error(f"review error: {str(e)}")
        raise e

@traceable
async def fit_item(user_info, style_data):
    try:
        chain = get_fit_chain()
        return chain.invoke({
            "user_info": user_info,
            "style_data": style_data
        })
    except exceptions.LangChainException as e:
        logger.error(f"fit error: {str(e)}")
        raise e