import uuid

from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from openai import AsyncAzureOpenAI
from bs4 import BeautifulSoup
from src.thread_safe.index import Index
from src.thread_safe.tslist import TsList

from azure.core.credentials import AzureKeyCredential


class AzureOpenAIConfig(BaseSettings):
    azure_deployment: str
    embedding_deployment: str
    index_name: str
    api_key: str
    azure_endpoint: str
    api_version: str

class AzureSearchClientConfig(BaseSettings):
    endpoint: str
    index_name: str
    key: str

class Messages:
    _response_index: Index = None
    _responses: TsList = None

    def __init__(self):
        self._responses = TsList()
        self._response_index = Index()

    def add(self, identity, message):
        position = self._responses.add(message)
        self._response_index.store_in_index(identity, position, message)
        return position

def clean_hts_context(search_results):
    # 1. Join snippets and parse
    context_html = "\n".join([doc['snippet'] for doc in search_results])
    soup = BeautifulSoup(context_html, 'html.parser')
    rows = soup.find_all('tr')

    cleaned_data = []
    seen_rows = set() # To prevent the same data appearing 3+ times

    for row in rows:
        # 2. Use a space separator (" ") to prevent words from mashing together
        # 3. Keep empty cells so the LLM knows which column is which
        cells = []
        for cell in row.find_all(['td', 'th']):
            text = cell.get_text(" ", strip=True) # "kg" + "No." = "kg No."
            cells.append(text if text else "-") # Use "-" for empty cells to maintain alignment

        # 4. Filter out headers and noise
        row_str = "| " + " | ".join(cells) + " |"

        # Avoid repeating headers (e.g., "Heading/ Subheading") or duplicate rows
        is_header = "Heading" in row_str or "Stat. Suf- fix" in row_str
        if row_str not in seen_rows and not is_header:
            cleaned_data.append(row_str)
            seen_rows.add(row_str)

    # 5. Return as a clean Markdown-style block
    return cleaned_data

class AzureAIClient:

    _identity: str = None
    _client: AsyncAzureOpenAI = None
    _search_client: SearchClient = None
    config: AzureOpenAIConfig = None

    def __init__(self, config: AzureOpenAIConfig, search_config: AzureSearchClientConfig = None):
        self._identity = uuid.uuid4().hex
        if config:
            self._client = AsyncAzureOpenAI(
                api_key=config.api_key,
                azure_endpoint=config.azure_endpoint,
                api_version=config.api_version,
                timeout=216,
                max_retries=2,
            )
            self.config = config


        if search_config:
            self._search_client = SearchClient(
                endpoint=search_config.endpoint,
                index_name=search_config.index_name,
                credential=AzureKeyCredential(search_config.key)
            )
            self.search_config= search_config


    @property
    def client(self):
        return self._client

    @property
    def files(self):
        return self._client.files

    @property
    def chat(self):
        return self._client.chat

    @property
    def search_client(self):
        return self._search_client

    async def get_embedding(self, text: str):
        dat = await self._client.embeddings.create(
            input=[text],
            model=self.config.embedding_deployment
        )
        return dat.data[0].embedding

    async def list_fine_tuning_jobs(self):
        jobs = [job async for job in await self.client.fine_tuning.jobs.list(limit=10)]
        return jobs

    async def get_fine_tuned_job(self):
        for j in await self.list_fine_tuning_jobs():
            yield await self.client.fine_tuning.jobs.retrieve(j.id)

    async def inject_vector_context(self, query, vector_query: str):
        query_vector = await self.get_embedding(vector_query)
        vector_query = VectorizedQuery(
            vector=query_vector,
            k_nearest_neighbors=2,
            weight=.1,
            fields="snippet_vector"
        )
        search_results = self._search_client.search(
            search_text=None,
            vector_queries=[vector_query],
            top=2,
            select=["snippet"]
        )
        cleaned_data = clean_hts_context(search_results)
        return "\n".join(cleaned_data)

    async def parse_response(self, query: str, vector_query, response_model: BaseModel, **kwargs):
        result = await self.client.responses.parse(
            model=self.config.azure_deployment,
            input=await self.inject_vector_context(query, vector_query),
            reasoning={"effort": "high"},
            text_format=response_model,
            **kwargs
        )
        return result


    async def ollama_parse_response(self, client, model, query: str, vector_query, response_model: BaseModel, **kwargs):
        result = await client.beta.chat.completions.parse(
            model=model,
            messages=[{"role": "user", "content": await self.inject_vector_context(query, vector_query)}],
            response_format=response_model,
            **kwargs
        )
        return result