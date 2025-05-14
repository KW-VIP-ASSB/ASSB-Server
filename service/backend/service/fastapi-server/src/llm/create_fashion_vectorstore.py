import os
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.document_loaders import TextLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from dotenv import load_dotenv
from pathlib import Path

base_dir = Path(__file__).resolve().parent
doc_path = base_dir / "fashion_docs" / "fashion_knowledge.txt"

load_dotenv()

embedding = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))

loader = TextLoader(str(doc_path), encoding='utf-8')
documents = loader.load()

splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
split_docs = splitter.split_documents(documents)

vectorstore = FAISS.from_documents(split_docs, embedding)
vectorstore.save_local("fashion_knowledge_db")

print("✅ fashion_knowledge_db 생성 완료")