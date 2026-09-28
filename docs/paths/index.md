# Learning Paths by Role

> Three guided routes through the handbook, one for each role: analytics engineer, data platform engineer and AI data engineer.

The guides are organised by topic. These paths reorder them by what a role does day to day, and pair each stage with a hands-on lab and a way to check that you are ready to move on. Every lab uses the same e-commerce dataset, so the work carries over from one stage to the next.

| | [Analytics engineer](analytics-engineer.md) | [Data platform engineer](platform-engineer.md) | [AI data engineer](ai-data-engineer.md) |
|-|---------------------------------------------|------------------------------------------------|------------------------------------------|
| **You own** | Trusted, modelled data that the business queries | The systems that move, store and run data reliably | The data and evaluation behind LLM applications |
| **Main tools** | SQL, a warehouse, dbt, BI | Kafka, Spark, Airflow, Kubernetes, Terraform | Embeddings, vector search, RAG, evals, agents |
| **Start here if** | You write SQL and want to model and test it well | You like infrastructure, streaming and operations | You build or feed LLM applications with private data |
| **Labs** | 01, 02, 08, 06 | 10, 04, 05, 03, 09, 06 | 07, 08 |
| **Lab time** | About 5 to 7 hours | About 9 to 12 hours | About 2 to 3 hours |

## How to use a path

1. **Read the guide, then do the lab.** The guides explain the ideas and show the patterns. The labs make you build and debug them, and each lab checks its own results.
2. **Use the checkpoint at the end of each stage.** If you cannot answer a question without looking, go back to that guide's *Common Pitfalls* and *Interview Questions* sections.
3. **Skip what you already know.** Every stage lists what it covers, so you can move past a stage you have done at work.
4. **Finish with a capstone.** The [capstone projects](../projects/index.md) combine several stages into one pipeline.

If none of these fits, the [topic-based paths on the home page](../README.md#learning-paths) cover beginners, warehouses, Spark, streaming and AI engineering. The [interview roadmap](../09-interviews/interview-roadmap.md) maps every topic to the guides that cover it.

Missing a role? Open a [topic request](https://github.com/sarangambekar1997/data-engineering-handbook/issues/new?template=topic-request.yml).
