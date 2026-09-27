# LangChain & LlamaIndex
> Frameworks for building RAG pipelines, agents, and LLM applications with less boilerplate.

**Prerequisites:** [RAG](rag.md) · [AI Agents](ai-agents.md)

**Related:** [AI Observability](ai-observability.md) · [Evals](eval-and-evals.md) · [Glossary](../99-reference/glossary.md)

---

## Overview

**Challenge:** LLM applications repeat the same integration work: loading documents, splitting them into chunks, embedding, writing to a vector store, retrieving, formatting prompts, calling the model, parsing output, and tracing. Building this from scratch for every project is slow, and changing model provider or vector database requires rewriting it.

**Solution:** these frameworks provide ready-made, interchangeable building blocks.
- **LangChain** focuses on composing steps — models, prompts, tools, and retrievers — into chains and agents (its LangGraph library handles stateful, multi-step agents).
- **LlamaIndex** focuses on *data*: ingesting documents, building indexes, and advanced retrieval for question-answering over your content.

```
            ┌── loaders (S3, Confluence, PDFs, SQL) ──┐
 your data ─┤   splitters · embeddings · vector stores ├──→ retriever ──→ prompt ──→ LLM ──→ parser
            └─────────── all interchangeable ──────────┘       (framework glue + tracing)
```

**Trade-offs:** frameworks speed up initial development and make components interchangeable, at the cost of additional abstraction, frequent API changes, and harder debugging. Many teams prototype with a framework and retain it only where it clearly saves effort.

---

## Table of Contents

**Basic**
- [LangChain vs LlamaIndex vs Raw SDK](#langchain-vs-llamaindex-vs-raw-sdk)
- [LangChain Setup](#langchain-setup)
- [LangChain Basics](#langchain-basics)

**Intermediate**
- [LangChain RAG Pipeline](#langchain-rag-pipeline)
- [LangChain Agents](#langchain-agents)
- [LlamaIndex Setup](#llamaindex-setup)
- [LlamaIndex RAG Pipeline](#llamaindex-rag-pipeline)

**Advanced**
- [LangChain Expression Language (LCEL)](#langchain-expression-language-lcel)
- [Custom Retrievers](#custom-retrievers)
- [LangSmith (Tracing & Evals)](#langsmith-tracing--evals)
- [When to Use a Framework vs Raw SDK](#when-to-use-a-framework-vs-raw-sdk)

**Reference**
- [Common Pitfalls](#common-pitfalls)
- [Cheat Sheet](#cheat-sheet)
- [Interview Questions](#interview-questions)
- [Further Reading](#further-reading)

---

## LangChain vs LlamaIndex vs Raw SDK

| | LangChain | LlamaIndex | Raw SDK |
|-|-----------|------------|---------|
| **Best for** | Chains, agents, flexible pipelines | RAG, document Q&A, knowledge bases | Full control, production, simple use cases |
| **RAG** | Good | Excellent | Manual but transparent |
| **Agents** | Excellent | Good | Manual via tool use |
| **Learning curve** | Steep | Medium | Low |
| **Abstraction level** | High | High | Low |
| **Vendor lock-in** | Some | Some | None |
| **When to choose** | Complex agent workflows | Document Q&A, data indexing | Most production use cases |

**Recommendation:** Start with the raw SDK. Add a framework only when you're writing the same boilerplate repeatedly.

---

## LangChain Setup

```bash
pip install langchain langchain-anthropic langchain-openai langchain-community langchain-text-splitters   # LangChain 1.x
pip install faiss-cpu                  # local vector store
pip install langchain-chroma           # Chroma vector store
```

```python
# LangChain works with both providers
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

llm        = ChatAnthropic(model="claude-sonnet-5")
llm_openai = ChatOpenAI(model="gpt-6-sol")
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
```

---

## LangChain Basics

### Invoke a model

```python
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

llm = ChatAnthropic(model="claude-sonnet-5")

# Simple call
response = llm.invoke("What is Apache Kafka?")
print(response.content)

# With system message
messages = [
    SystemMessage(content="You are a concise data engineering expert."),
    HumanMessage(content="What is Apache Kafka?"),
]
response = llm.invoke(messages)
print(response.content)
```

### Prompt templates

```python
from langchain_core.prompts import ChatPromptTemplate

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are an expert in {domain}. Be concise."),
    ("human",  "{question}"),
])

chain = prompt | llm
response = chain.invoke({"domain": "data engineering", "question": "What is a DAG?"})
print(response.content)
```

### Output parsers

```python
from langchain_core.output_parsers import StrOutputParser
from pydantic import BaseModel      # langchain_core.pydantic_v1 was removed in langchain-core 0.3

# String output
chain = prompt | llm | StrOutputParser()
result = chain.invoke({"domain": "SQL", "question": "Explain a CTE"})
print(result)  # plain string

# Structured JSON output
class PipelineInfo(BaseModel):
    name:        str
    schedule:    str
    source:      str
    destination: str

# with_structured_output uses the provider's native structured output / tool calling —
# more reliable than asking for JSON in the prompt and parsing it
structured_llm = llm.with_structured_output(PipelineInfo)

structured_prompt = ChatPromptTemplate.from_messages([
    ("system", "Extract the pipeline details."),
    ("human",  "{text}"),
])

chain = structured_prompt | structured_llm
result = chain.invoke({"text": "Nightly job that loads payments from the billing API into the warehouse at 2am UTC"})
print(result)  # PipelineInfo(name=..., schedule=..., ...)
```

---

## LangChain RAG Pipeline

```python
from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

# ── 1. Load documents ─────────────────────────────────────────────────────────
loader = DirectoryLoader("./docs/", glob="**/*.md", loader_cls=TextLoader)
docs   = loader.load()
print(f"Loaded {len(docs)} documents")

# ── 2. Split into chunks ──────────────────────────────────────────────────────
splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=50,
    separators=["\n\n", "\n", " ", ""]
)
chunks = splitter.split_documents(docs)
print(f"Split into {len(chunks)} chunks")

# ── 3. Embed and store ────────────────────────────────────────────────────────
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
vectorstore = Chroma.from_documents(
    documents=chunks,
    embedding=embeddings,
    persist_directory="./chroma_db"
)

# ── 4. Create retriever ───────────────────────────────────────────────────────
retriever = vectorstore.as_retriever(
    search_type="similarity",
    search_kwargs={"k": 5}
)

# ── 5. Build RAG chain ────────────────────────────────────────────────────────
llm = ChatOpenAI(model="gpt-6-luna", temperature=0)

prompt = ChatPromptTemplate.from_template("""
Answer based only on the context below.
If the answer isn't there, say "I don't have that information."

Context:
{context}

Question: {question}
""")

def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)

rag_chain = (
    {"context": retriever | format_docs, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)

# ── 6. Ask questions ──────────────────────────────────────────────────────────
answer = rag_chain.invoke("What is the medallion architecture?")
print(answer)

# Stream
for chunk in rag_chain.stream("Explain PySpark window functions"):
    print(chunk, end="", flush=True)
```

### With metadata filtering

```python
from langchain_chroma import Chroma

retriever = vectorstore.as_retriever(
    search_type="similarity",
    search_kwargs={
        "k": 5,
        "filter": {"source": "docs/pyspark-reference.md"}  # Chroma metadata filter
    }
)
```

---

## LangChain Agents

```python
from langchain_anthropic import ChatAnthropic
from langchain.agents import create_agent          # LangChain 1.0+
from langchain_core.tools import tool

# ── Define tools with @tool decorator ─────────────────────────────────────────
@tool
def run_sql(query: str) -> str:
    """Execute a SQL SELECT query against the data warehouse. Returns results as JSON."""
    # Simulated
    return '{"rows": [{"count": 152340}]}'

@tool
def get_table_schema(table_name: str) -> str:
    """Get the schema (columns and types) for a given table."""
    schemas = {
        "orders": "order_id VARCHAR, customer_id INT, amount DECIMAL, status VARCHAR"
    }
    return schemas.get(table_name, f"Table {table_name} not found")

@tool
def check_pipeline_status(pipeline_name: str) -> str:
    """Check whether a data pipeline ran successfully recently."""
    return f"{pipeline_name}: last run 2024-03-15 02:15 UTC, status=SUCCESS, rows=15234"

tools = [run_sql, get_table_schema, check_pipeline_status]

# ── Build agent ────────────────────────────────────────────────────────────────
# LangChain 1.0 replaced AgentExecutor / create_tool_calling_agent (now in the
# langchain-classic package) with create_agent, which runs on LangGraph.
llm = ChatAnthropic(model="claude-sonnet-5")

agent = create_agent(
    model=llm,
    tools=tools,
    system_prompt="You are a data engineering assistant. Use tools to answer accurately.",
)

result = agent.invoke(
    {"messages": [{"role": "user", "content": "How many orders do we have and is the pipeline healthy?"}]},
    config={"recursion_limit": 20},     # cap the number of agent steps
)
print(result["messages"][-1].text)     # final answer text
```

---

## LlamaIndex Setup

```bash
pip install llama-index llama-index-llms-anthropic llama-index-llms-openai
pip install llama-index-embeddings-openai llama-index-vector-stores-chroma
```

```python
from llama_index.llms.anthropic import Anthropic
from llama_index.llms.openai import OpenAI
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import Settings

# Global settings
Settings.llm       = Anthropic(model="claude-sonnet-5")
Settings.embed_model = OpenAIEmbedding(model="text-embedding-3-small")
Settings.chunk_size    = 512
Settings.chunk_overlap = 50
```

---

## LlamaIndex RAG Pipeline

```python
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.core import StorageContext, load_index_from_storage
from llama_index.core.node_parser import SentenceSplitter
from pathlib import Path

# ── Build index from docs directory ───────────────────────────────────────────
documents = SimpleDirectoryReader("./docs/").load_data()

parser = SentenceSplitter(chunk_size=512, chunk_overlap=50)
nodes  = parser.get_nodes_from_documents(documents)

index = VectorStoreIndex(nodes)

# ── Save & load ────────────────────────────────────────────────────────────────
index.storage_context.persist("./storage")

# Load from disk
storage_context = StorageContext.from_defaults(persist_dir="./storage")
index = load_index_from_storage(storage_context)

# ── Query ──────────────────────────────────────────────────────────────────────
query_engine = index.as_query_engine(
    similarity_top_k=5,
    response_mode="compact"   # "compact" | "refine" | "tree_summarize"
)

response = query_engine.query("What is the medallion architecture?")
print(response.response)

# See source nodes
for node in response.source_nodes:
    print(f"Score: {node.score:.3f}  Source: {node.metadata.get('file_name', 'unknown')}")
    print(f"  {node.text[:200]}\n")

# ── Chat mode (multi-turn) ─────────────────────────────────────────────────────
chat_engine = index.as_chat_engine(chat_mode="condense_plus_context")
response = chat_engine.chat("What is Kafka?")
print(response.response)
response = chat_engine.chat("How does it compare to RabbitMQ?")
print(response.response)
```

### With metadata filtering

```python
from llama_index.core.vector_stores import MetadataFilter, MetadataFilters

filters = MetadataFilters(filters=[
    MetadataFilter(key="file_name", value="pyspark-reference.md")
])
query_engine = index.as_query_engine(filters=filters, similarity_top_k=3)
response = query_engine.query("Explain window functions")
```

### Sub-question query engine (multi-document)

```python
from llama_index.core.query_engine import SubQuestionQueryEngine
from llama_index.core.tools import QueryEngineTool

# Build separate indexes per document
pyspark_engine = VectorStoreIndex.from_documents(pyspark_docs).as_query_engine()
airflow_engine = VectorStoreIndex.from_documents(airflow_docs).as_query_engine()

tools = [
    QueryEngineTool.from_defaults(query_engine=pyspark_engine, name="pyspark",
                                   description="PySpark documentation"),
    QueryEngineTool.from_defaults(query_engine=airflow_engine, name="airflow",
                                   description="Airflow documentation"),
]

sub_question_engine = SubQuestionQueryEngine.from_defaults(query_engine_tools=tools)
response = sub_question_engine.query(
    "Compare Spark Structured Streaming with Airflow scheduling for batch workloads"
)
print(response.response)
```

---

## LangChain Expression Language (LCEL)

LCEL uses the `|` operator to compose chains — similar to Unix pipes.

```python
from langchain_core.runnables import RunnableParallel, RunnableLambda

# ── Sequential chain ───────────────────────────────────────────────────────────
chain = prompt | llm | StrOutputParser()

# ── Parallel execution ─────────────────────────────────────────────────────────
parallel_chain = RunnableParallel({
    "summary":    prompt_summary  | llm | StrOutputParser(),
    "key_points": prompt_bullets  | llm | StrOutputParser(),
})
result = parallel_chain.invoke({"text": long_document})

# ── With fallback ──────────────────────────────────────────────────────────────
primary  = ChatAnthropic(model="claude-sonnet-5")
fallback = ChatOpenAI(model="gpt-6-luna")

chain_with_fallback = (prompt | primary | StrOutputParser()).with_fallbacks(
    [prompt | fallback | StrOutputParser()]
)

# ── Add retry logic ────────────────────────────────────────────────────────────
reliable_llm = llm.with_retry(stop_after_attempt=3, wait_exponential_jitter=True)

# ── Conditional routing ────────────────────────────────────────────────────────
def route(inputs):
    if "sql" in inputs["question"].lower():
        return sql_chain
    elif "pipeline" in inputs["question"].lower():
        return pipeline_chain
    return general_chain

routed_chain = RunnableLambda(route)
```

---

## Custom Retrievers

```python
from langchain_core.retrievers import BaseRetriever
from langchain_core.documents import Document

class HybridRetriever(BaseRetriever):
    """Combines vector search and keyword search."""

    vectorstore: object
    bm25_index:  object
    k:           int = 5
    alpha:       float = 0.5  # 0=BM25, 1=vector

    def _get_relevant_documents(self, query: str) -> list[Document]:
        # Vector results
        vec_docs  = self.vectorstore.similarity_search(query, k=self.k * 2)
        # BM25 results
        bm25_docs = self.bm25_index.get_top_n(query.split(), self.k * 2)

        # Reciprocal Rank Fusion
        scores = {}
        for rank, doc in enumerate(vec_docs):
            key = doc.page_content[:50]
            scores[key] = scores.get(key, 0) + self.alpha * (1 / (rank + 1))
        for rank, doc in enumerate(bm25_docs):
            key = doc.page_content[:50]
            scores[key] = scores.get(key, 0) + (1 - self.alpha) * (1 / (rank + 1))

        all_docs = {d.page_content[:50]: d for d in vec_docs + bm25_docs}
        ranked   = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [all_docs[k] for k, _ in ranked[:self.k] if k in all_docs]
```

---

## LangSmith (Tracing & Evals)

LangSmith traces every LLM call — inputs, outputs, latency, cost — for debugging and evaluation.

```bash
pip install langsmith
```

```python
import os
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"]    = "ls__..."
os.environ["LANGCHAIN_PROJECT"]    = "de-bible-rag"

# All LangChain calls are now automatically traced
result = rag_chain.invoke("What is Kafka?")
# Visit https://smith.langchain.com to see the trace
```

```python
# Run evaluations
from langsmith import Client
from langsmith.evaluation import evaluate, LangChainStringEvaluator

client = Client()

# Create a dataset
dataset = client.create_dataset("de-rag-eval")
client.create_examples(
    inputs=[
        {"question": "What is the medallion architecture?"},
        {"question": "How does Kafka guarantee delivery?"},
    ],
    outputs=[
        {"answer": "Bronze/Silver/Gold layers"},
        {"answer": "At-least-once by default, exactly-once with transactions"},
    ],
    dataset_id=dataset.id
)

# Evaluate
results = evaluate(
    lambda inputs: {"output": rag_chain.invoke(inputs["question"])},
    data=dataset.name,
    evaluators=[LangChainStringEvaluator("cot_qa")],
    experiment_prefix="rag-v1"
)
```

---

## When to Use a Framework vs Raw SDK

```
Use the raw SDK when:
  - Simple use case (single LLM call, basic RAG)
  - Production code where you need full control
  - You're optimizing for latency or cost
  - Your team doesn't already know the framework

Use LangChain when:
  - Building complex agent workflows
  - Need to swap LLM providers easily
  - Want built-in streaming, retries, fallbacks
  - Already using LangSmith for tracing

Use LlamaIndex when:
  - Building document Q&A or knowledge base
  - Need advanced retrieval (sub-question, HyDE, re-ranking)
  - Working with large document collections
  - Want higher-level RAG abstractions out of the box
```

---

## Common Pitfalls

| Pitfall | Symptom | Fix |
|---------|---------|-----|
| Copying tutorials written for older versions | `ImportError`s: `langchain.text_splitter`, `pydantic_v1`, `ServiceContext`, `AgentExecutor` | Check the version; use the split packages (`langchain-*` integrations, `langchain-text-splitters`) and current APIs (`create_agent`, `Settings`) |
| Unpinned framework versions | A minor upgrade breaks production | Pin exact versions; upgrade deliberately with tests |
| Default chunking and retrieval settings | Mediocre answers that no prompt fixes | Tune chunk size, k, and re-ranking on an eval set |
| Parsing JSON from free text | Intermittent parse failures | `llm.with_structured_output(PydanticModel)` (LangChain) or structured outputs in LlamaIndex |
| Abstractions hiding the actual prompt | Can't tell why the model behaved oddly | Turn on tracing (LangSmith, Langfuse, OpenTelemetry) and read the real prompts |
| Agents with no step limit | Runaway loops and bills | `recursion_limit` / `max_iterations`, and cost guards |
| Rebuilding the index on every app start | Slow startup; repeated embedding costs | Persist the index or vector store; load it instead of rebuilding |
| Using a framework for a single API call | Extra dependencies and complexity for no gain | Call the provider SDK directly |

---

## Cheat Sheet

**LangChain (1.x)**

| Task | Code |
|------|------|
| Chat model | `ChatAnthropic(model="claude-sonnet-5")` · `ChatOpenAI(model=...)` |
| Call it | `llm.invoke("question")` → `.text` · `llm.stream(...)` · `llm.batch([...])` |
| Prompt + model + parser (LCEL) | `chain = prompt \| llm \| StrOutputParser()` → `chain.invoke({...})` |
| Structured output | `llm.with_structured_output(MyPydanticModel)` |
| Split text | `RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100).split_documents(docs)` |
| Vector store → retriever | `Chroma.from_documents(docs, embeddings).as_retriever(search_kwargs={"k": 5})` |
| Tool | `@tool` on a typed function with a docstring |
| Agent | `create_agent(model=llm, tools=[...], system_prompt="...")` → `.invoke({"messages": [...]})` |
| Fallback model | `primary.with_fallbacks([backup])` |
| Retries | `llm.with_retry(stop_after_attempt=3)` |

**LlamaIndex**

| Task | Code |
|------|------|
| Global model settings | `Settings.llm = Anthropic(model="claude-sonnet-5")` · `Settings.embed_model = ...` |
| Load documents | `SimpleDirectoryReader("docs/").load_data()` |
| Build an index | `VectorStoreIndex.from_documents(docs)` |
| Ask questions | `index.as_query_engine(similarity_top_k=5).query("...")` |
| Chat over data | `index.as_chat_engine()` |
| Persist / load | `index.storage_context.persist("./storage")` · `load_index_from_storage(StorageContext.from_defaults(persist_dir="./storage"))` |
| Filter | `MetadataFilters(filters=[MetadataFilter(key="source", value="runbooks")])` |

**Choosing:** a single call or simple extraction → provider SDK · RAG over many documents → LlamaIndex · multi-step agents with state, branching, and human approval → LangGraph / LangChain · heavy production requirements → often your own thin layer on the SDK

---

## Interview Questions

**Q: When would you use LangChain or LlamaIndex, and when would you use the raw SDK?**
A: Use a framework when it removes real work: many document loaders and vector-store integrations, standard RAG patterns, provider swapping, or agent orchestration with state and checkpoints. Use the raw SDK for simple or performance-critical paths where you want full control over prompts, retries, and costs, fewer dependencies, and the newest provider features the day they launch. A common pattern is to prototype with a framework and harden the critical path on the SDK.

**Q: What is LCEL?**
A: The LangChain Expression Language composes components ("runnables") with the pipe operator — `prompt | llm | parser` — into a chain. Every runnable has the same interface (`invoke`, `batch`, `stream`, and async versions), so a composed chain automatically supports streaming, batching, parallel branches (`RunnableParallel`), fallbacks, and retries, and shows up in tracing.

**Q: How does LlamaIndex build and query an index?**
A: It loads documents into `Document` objects, splits them into nodes (chunks) with a node parser, embeds each node, and stores them in a vector store behind a `VectorStoreIndex`. At query time, a retriever finds the top-k similar nodes (optionally filtered and re-ranked), and a response synthesizer puts them into a prompt and calls the LLM. Query engines like the sub-question engine break complex questions into smaller ones across several indexes.

**Q: How do you debug a LangChain or LlamaIndex application?**
A: Tracing first: LangSmith, Langfuse, or an OpenTelemetry-based tool shows every step — the rendered prompts, retrieved chunks, tool calls, token counts, and latencies. Check retrieval separately from generation (did the right chunks come back?), reproduce failures with the exact inputs from the trace, and add them to an evaluation set so the fix is locked in.

---

## Further Reading

- [LangChain documentation](https://docs.langchain.com/) and [LangGraph](https://langchain-ai.github.io/langgraph/)
- [LangChain v1 migration guide](https://docs.langchain.com/oss/python/migrate/langchain-v1)
- [LlamaIndex documentation](https://docs.llamaindex.ai/)
- [LangSmith](https://docs.smith.langchain.com/) — tracing and evaluation
- [Anthropic: Building effective agents](https://www.anthropic.com/engineering/building-effective-agents) — when frameworks help and when they hide too much

---

**Previous:** [AI Agents](ai-agents.md) · **Next:** [Evals](eval-and-evals.md) · **Back to:** [Index](../README.md)
