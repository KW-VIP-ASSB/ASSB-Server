from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.runnables import RunnableMap, RunnableLambda, RunnableSequence
from src.llm.prompt import review_prompt, fit_prompt

import os
from langchain.chains import RetrievalQA
from src.llm.init_store import load_vectorstore
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.7,
    openai_api_key=OPENAI_API_KEY,
    model_kwargs={"response_format": {"type": "json_object"}}
)

review_chain = review_prompt | llm | JsonOutputParser()


def get_fit_chain():
    retriever = load_vectorstore().as_retriever(search_type="similarity", search_kwargs={"k": 3})

    query_extractor = RunnableLambda(lambda x: x["style_data"]["product_name"])
    context_retriever = query_extractor | retriever

    input_map = RunnableMap({
        "context": context_retriever,
        "user_info": lambda x: x["user_info"],
        "style_data": lambda x: x["style_data"]
    })

    format_prompt = RunnableLambda(lambda inputs: fit_prompt.format_prompt(
        user_info=inputs["user_info"],
        style_data=inputs["style_data"],
        context="\n\n".join(doc.page_content for doc in inputs["context"])
    ))

    llm = ChatOpenAI(
        model="gpt-4o-mini",
        temperature=0.7,
        openai_api_key=OPENAI_API_KEY,
        model_kwargs={"response_format": {"type": "json_object"}}
    )

    return input_map | format_prompt | llm | JsonOutputParser()