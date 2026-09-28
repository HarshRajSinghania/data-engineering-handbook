# AI Data Engineer Path

> From LLM APIs and embeddings to retrieval, agents with safe data access, and the evaluation that shows whether any of it works.

**Who it is for:** you build or feed LLM applications with private data, and you need them to be measurable, observable and safe.

**Labs in this path:** 07 and 08, about 2 to 3 hours in total. Both run locally with Python only.

## Stage 1 — Foundations

*Goal: call a model reliably and control its output.*

- **Read:** [Prompt Engineering](../07-ai/prompt-engineering.md), then [LLM APIs and SDKs](../07-ai/llm-apis.md).
- **Also:** [Python for DE](../00-foundations/python-reference.md) and [SQL](../00-foundations/sql-reference.md), if you are new to either.

**Checkpoint:** explain how you would enforce a JSON output shape, and what your code does when the model returns something else.

## Stage 2 — Retrieval

*Goal: give a model the right context from your own documents.*

- **Read:** [Embeddings](../07-ai/embeddings.md), [Vector Databases](../07-ai/vector-databases.md) and [RAG](../07-ai/rag.md).
- **Do:** [Lab 07, Capstone: Docs RAG](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/07-docs-rag) (60–90 minutes): chunking, a BM25 index and a retrieval evaluation (hit rate and MRR) over this handbook's own guides.

**Checkpoint:** retrieval misses a document that contains the answer. List the causes, from chunking to ranking, and how you would tell them apart.

## Stage 3 — Agents and data access

*Goal: let a model use tools and query data without putting the data at risk.*

- **Read:** [AI Agents and Tool Use](../07-ai/ai-agents.md), then [MCP and Text-to-SQL](../07-ai/mcp-text-to-sql.md). [LangChain and LlamaIndex](../07-ai/langchain-llamaindex.md) cover orchestration libraries.

**Checkpoint:** an assistant can run SQL on your warehouse. List the controls that make that acceptable (read-only access, allowed tables, limits), and how you would test that each one holds.

## Stage 4 — Evaluation and observability

*Goal: measure quality before and after release.*

- **Read:** [Evals](../07-ai/eval-and-evals.md), then [AI Observability](../07-ai/ai-observability.md).
- **Do:** [Lab 08, Data Quality Gates](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/08-data-quality) (60–90 minutes). The same idea, checks with thresholds that block a release, applies to the data that feeds a model. Read [Data Quality](../05-quality-governance/data-quality.md) alongside it.

**Checkpoint:** design an evaluation set for a support chatbot: how many questions, how they are labelled, and what result would stop a release.

## Stage 5 — Operating models

*Goal: track experiments, and choose between prompting, retrieval, fine-tuning and local models.*

- **Read:** [MLflow](../07-ai/mlflow.md), [Fine-Tuning LLMs](../07-ai/fine-tuning.md) and [Local LLMs](../07-ai/local-llms.md).
- **Also:** [Claude Code](../07-ai/claude-code.md), for building with an AI coding assistant.

**Checkpoint:** for one task, say why you would prompt, retrieve, fine-tune or run a local model, and what evidence would change your mind.

## Stage 6 — The data underneath

*Goal: keep the data behind the application fresh, governed and secure.*

- **Read:** [Data Ingestion and CDC](../02-processing/ingestion-cdc.md), [Data Governance and Lineage](../05-quality-governance/governance-lineage.md) and [Data Security and Privacy](../05-quality-governance/data-security-privacy.md).

**You are done when** you can explain, for an application of your choice, where each answer comes from, how you know retrieval and generation are good enough, and what data the system must never expose.

## Going further

- Learn the pipelines that feed a retrieval index with the [data platform engineer path](platform-engineer.md).
- Extend Lab 07 with a vector index, and compare its hit rate with BM25.
