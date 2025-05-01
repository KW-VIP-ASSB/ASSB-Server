import os
from dotenv import load_dotenv
from typing import Dict, List, Any
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI
from langchain_core import exceptions
from langchain.globals import set_llm_cache
from langchain.cache import SQLiteCache
from langchain_core.prompts import ChatPromptTemplate
from langsmith import traceable
import asyncio
from logging import getLogger

logger = getLogger(__name__)

# Load environment variables
load_dotenv()

# Setup LLM cache
set_llm_cache(SQLiteCache(database_path=".langchain.db"))

# Configure OpenAI client
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

# Initialize LLM
llm = ChatOpenAI(
    seed=311, 
    temperature=0.7, 
    model="gpt-4o-mini", 
    openai_api_key=OPENAI_API_KEY, 
    model_kwargs={"response_format": {"type": "json_object"}}
)

# Review summarization prompt
review_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        """
        당신은 패션 제품 리뷰를 분석하는 AI입니다. 주어진 리뷰 텍스트를 분석하여 다음 정보를 요약해주세요:

        1. 전반적인 평점(평균 평점) 및 만족도 요약
           - 추천해요 / 비추천해요
           - 만족해요 / 보통이에요 / 최악이에요
        2. 자주 언급되고 있는 제품의 장점
           - 예: 사이즈가 적당해요, 소재가 좋아요
        3. 자주 언급되고 있는 제품의 단점
           - 예: 이미지와 색이 달라요, 배송이 느려요
        
        사용자 정보도 함께 고려하여 맞춤형 분석을 제공하세요.
        
        답변은 다음 JSON 형식으로 제공하세요:
        {
            "overall_rating": {
                "score": "평균 평점(5점 만점)",
                "recommendation": "추천해요 또는 비추천해요",
                "satisfaction": "만족해요, 보통이에요, 최악이에요 중 하나"
            },
            "pros": ["장점1", "장점2", "장점3", ...],
            "cons": ["단점1", "단점2", "단점3", ...],
            "summary": "전체적인 리뷰 요약"
        }
        """
    ),
    ("human", "사용자 정보: {user_info}\n\n리뷰 텍스트: {review_text}"),
])

# Fit analysis prompt
fit_prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        """
        당신은 패션 제품 핏 분석 AI입니다. 제공된 제품 정보와 사용자 정보를 분석하여 다음 항목을 평가해주세요:

        1. 사이즈 관련 피드백 요약
           - 크게 나옴 / 작게 나옴 / 정사이즈
        2. 제품의 계절감
           - 봄에 입기 좋아요 / 여름에 입기 좋아요 / 가을에 입기 좋아요 / 겨울에 입기 좋아요
        3. 체형 적합성
           - 어떤 체형에 잘 어울리는지
        4. 사용자의 현재 스타일과 추구하는 스타일과의 적합성
        5. 핏 유형
           - 스탠다드 핏 / 오버사이즈 핏 / 슬림 핏 중 어떤 것에 가까운지
        
        답변은 다음 JSON 형식으로 제공하세요:
        {
            "size_feedback": "크게 나옴/작게 나옴/정사이즈",
            "season": ["적합한 계절1", "적합한 계절2", ...],
            "body_type": ["적합한 체형1", "적합한 체형2", ...],
            "style_compatibility": {
                "score": "1-10 점수",
                "explanation": "스타일 적합성에 대한 설명"
            },
            "fit_type": "스탠다드 핏/오버사이즈 핏/슬림 핏",
            "recommendation": "사용자에게 제품을 추천하는지 여부와 그 이유"
        }
        """
    ),
    ("human", "사용자 정보: {user_info}\n\n제품 정보: {style_data}"),
])

# Create chains with output parser
review_chain = review_prompt | llm | JsonOutputParser()
fit_chain = fit_prompt | llm | JsonOutputParser()

@traceable
async def summarize_review(user_info: Dict[str, Any], review_text: str) -> Dict[str, Any]:
    """
    Summarize product reviews using LLM.
    
    Args:
        user_info: User information dictionary
        review_text: Concatenated review texts
        
    Returns:
        Dict containing review summary
    """
    try:
        result = review_chain.invoke({
            "user_info": user_info,
            "review_text": review_text
        })
        return result
    except exceptions.LangChainException as e:
        logger.error(f"LangChain Error in summarize_review: {str(e)}")
        raise e

@traceable
async def fit_item(user_info: Dict[str, Any], style_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze if an item fits a user based on style data and user info.
    
    Args:
        user_info: User information dictionary
        style_data: Style data dictionary
        
    Returns:
        Dict containing fit analysis
    """
    try:
        result = fit_chain.invoke({
            "user_info": user_info,
            "style_data": style_data
        })
        return result
    except exceptions.LangChainException as e:
        logger.error(f"LangChain Error in fit_item: {str(e)}")
        raise e
