from langchain.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
import os
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def load_vectorstore(path: str = "src/llm/fashion_knowledge_db"):
    embedding = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    return FAISS.load_local(path, embedding, allow_dangerous_deserialization=True)