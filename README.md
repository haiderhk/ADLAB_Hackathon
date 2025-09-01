# Insight Agent for Snowflake (RAG-powered Analytics)

A multi-user Streamlit web app that uses a Retrieval-Augmented Generation (RAG) architecture to answer business questions about your Snowflake data warehouse. It extracts and enriches Snowflake metadata, stores it as embeddings in ChromaDB and as a graph, and uses GPT to generate insights, SQL, and charts with role-based UI.

## Highlights
- Multi-user login with roles: Admin, Analyst, Executive (via `users.txt`)
- Metadata extraction from Snowflake with enrichment (descriptions, joins, table types, indexes)
- Vector search (ChromaDB) + Graph search (NetworkX / optional Neo4j)
- RAG pipeline: retrieve → prompt GPT → generate insight + SQL + chart suggestion → execute → visualize
- Role-based Streamlit UI with tabs for Insight, SQL, and Chart
- Nightly job to refresh metadata and embeddings
- Caching for common Q&A
- Robust logging and error handling

---

## Architecture Overview

```mermaid
graph TD
  A[Snowflake] -->|information_schema / account_usage| B[Metadata Extractor]
  B -->|tables, columns, stats, fkeys, indexes| C[JSON Snapshot data/metadata_latest.json]
  B -->|docs (text chunks)| D[ChromaDB]
  B -->|nodes+edges| E[Local Graph JSON data/graphdb.json]
  E -->|optional upsert| F[Neo4j]
  G[Streamlit UI] -->|question| H[RAG Pipeline]
  H -->|retrieve| D
  H -->|fallback search| E
  H -->|context+question| I[GPT]
  I -->|insight+sql+chart_type| H
  H -->|execute sql| A
  H -->|dataframe| G
  G -->|charts + tables| User
```

---

## Key Components (Files)

- `app.py`: Streamlit UI, role-based navigation, Ask/Metadata/Admin pages
- `rag_pipeline.py`: Retrieval (Chroma + graph), GPT prompting, SQL execution, chart heuristics, caching
- `metadata_extractor.py`: Snowflake connector, context setup, metadata queries, column stats, JSON-safe persistence, graph build
- `metadata_queries.py`: Extensible set of SQL queries to extract metadata (tables, columns, descriptions, foreign keys, indexes, query history)
- `graphdb_utils.py`: Graph utilities (NetworkX local JSON), optional Neo4j upsert
- `auth.py`: Reads `users.txt`, verifies credentials and roles
- `config.py`: Centralized configuration and env variables
- `refresh_metadata.py`: CLI for nightly metadata refresh and embedding upsert
- `users.txt`: User credentials and roles
- `requirements.txt`: Python dependencies
- `data/`: App-generated snapshots and graph
  - `metadata_latest.json`: Current structured metadata snapshot
  - `metadata_docs.json`: Text documents used for embeddings (RAG context)
  - `graphdb.json`: Local directed graph of DB → schema → table → columns
- `chroma_db/`: Persistent Chroma collection storage
- `logs/app.log`: Runtime logs (queries, errors, traces)

---

## Roles & Permissions

- Executive: Sees high-level insight + chart. No SQL tab.
- Analyst: Sees insight + SQL + chart; can explore Metadata page.
- Admin: Full access; can Refresh Metadata, view logs, upload CSV into Snowflake.

`users.txt` format:
```
username:password:Role
admin:admin123:Admin
analyst:analyst123:Analyst
exec:exec123:Executive
```

Optionally hash passwords with SHA256 by prefixing `{SHA256}` in the file, for example:
```
admin:{SHA256}5e884898da28047151d0e56f8dc6292773603d0d...:Admin
```

---

## Configuration

Set environment variables (recommended via `.env`):
```
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=        # optional; if omitted, account_usage fallbacks are used
SNOWFLAKE_SCHEMA=          # optional
SNOWFLAKE_ROLE=            # optional
OPENAI_API_KEY=            # required for LLM + embeddings
CHROMA_DIR=./chroma_db
GRAPH_PATH=./data/graphdb.json
USERS_FILE=./users.txt
LOG_FILE=./logs/app.log
OPENAI_MODEL=gpt-4o-mini
OPENAI_EMBEDDING_MODEL=text-embedding-3-large
TOP_K=8
```

---

## Metadata Extraction & Enrichment

- Connects using `snowflake-connector-python`. Context is applied explicitly:
  - `USE ROLE`, `USE WAREHOUSE`, `USE DATABASE`, `USE SCHEMA` if provided
- Extracted from:
  - `information_schema.*` (when `SNOWFLAKE_DATABASE` is set)
  - `snowflake.account_usage.*` fallback (when DB context is not set)
- Collected metadata:
  - Tables: `database_name`, `schema_name`, `table_name`, `table_type`, `row_count`, `created`, `last_altered`, `table_description`, `table_category` (naive fact/dimension hint via name pattern)
  - Columns: data type, length/precision/scale, nullability, `column_description`
  - Foreign keys: child and referenced tables/columns (join topology)
  - Indexes (account_usage): index_name, column_name
  - Query history (sample): text, status, elapsed time
- Stats sampling:
  - For a subset of columns per table, compute min/max/distinct counts.
- Persistence:
  - `data/metadata_latest.json`: Structured metadata; JSON-safe (decimals → float, datetimes → ISO8601)
  - `data/metadata_docs.json`: Flattened text docs for embeddings (tables, columns, stats, queries)
  - `data/graphdb.json`: Graph of DB → schema → table → columns (NetworkX node-link JSON)

Extend or customize by editing `metadata_queries.py` (add/modify queries). The extractor will pick them up automatically.

---

## Vector Store (ChromaDB)

- Collection name: `metadata_docs`
- Documents: Textual summaries of tables, columns, stats, and recent queries
- Embeddings: OpenAI (`text-embedding-3-large`) by default
- Persistence: `./chroma_db`
- Upsert is batched; duplicate IDs are handled by design of `docs` IDs. If you add your own docs, ensure globally unique `id` per record.

---

## Graph Store (NetworkX JSON + optional Neo4j)

- Local: `data/graphdb.json` using NetworkX node-link JSON
- Nodes: `Database`, `Schema`, `Table`, `Column`
- Edges: `CONTAINS`, `HAS_COLUMN`
- Optional Neo4j upsert if `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` are set
- Used for fallback contextual search and future relationship reasoning (e.g., joins)

---

## RAG Pipeline

1. Retrieve relevant context
   - Query ChromaDB with the natural-language question (`TOP_K` results)
   - If vector recall is sparse, fallback to graph keyword search
2. Build prompt
   - System: “expert Snowflake analyst” with strict instructions
   - User: Includes metadata snippets and the question; asks for JSON response with keys: `insight`, `sql`, `chart_type`
3. Generate with GPT
   - Model: configurable (`OPENAI_MODEL`)
   - Parse JSON robustly; if parsing fails, return insight-only fallback
4. Execute SQL (if provided)
   - Apply Snowflake context, run query, fetch pandas DataFrame
5. Visualize
   - Heuristics for chart type and axes (line/area for time series, bar for category, scatter for numeric)
6. Summarize
   - Display quick stats (row count, min/max, distincts)

Caching: Results cached with `diskcache` to speed up repeated questions.

---

## Streamlit UI (Role-based)

- Ask:
  - Executive: Insight + Chart
  - Analyst/Admin: Insight + SQL + Chart
- Metadata:
  - Test connection
  - Latest Metadata Snapshot (tables, columns, stats)
  - Knowledge Base Report (full docs powering RAG) with search + download
  - Admin-only: Refresh metadata (re-extract, re-index, regraph)
  - Debug expander: CWD and `./data` dir listing
- Admin:
  - Logs viewer (`logs/app.log`)
  - CSV Upload to Snowflake using `write_pandas` with `auto_create_table=True`

---

## Setup & Run

1. Create virtual environment and install dependencies
```
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip wheel setuptools
pip install -r requirements.txt
```

2. Configure environment
```
cp .env.example .env   # if you created one, otherwise fill `.env`
# Fill SNOWFLAKE_* and OPENAI_API_KEY
```

3. Start the app
```
streamlit run app.py
```

4. Login with `users.txt` credentials
- Admin: `admin / admin123`
- Analyst: `analyst / analyst123`
- Executive: `exec / exec123`

5. Admin → Metadata → Refresh metadata
- Confirms connection, writes snapshots, indexes docs, builds graph

---

## Nightly Auto-Update

Use `cron` (Linux) or a scheduler to refresh nightly:
```
# m h  dom mon dow  command
0 2 * * *  cd /home/youruser/Adlabs/Hackathon/Agent && \
  /home/youruser/Adlabs/Hackathon/Agent/.venv/bin/python refresh_metadata.py >> logs/app.log 2>&1
```

---

## Troubleshooting

- Connection OK but no metadata files
  - Verify `SNOWFLAKE_*` values and role privileges to `information_schema` or `snowflake.account_usage`
- JSON serialization errors (Decimals/datetimes)
  - The app converts to JSON-safe types; update to latest code if you see this
- Chroma DuplicateIDError
  - Ensure each document `id` is unique; built-in docs are unique, custom docs must be as well
- LLM JSON parse failed
  - We fall back to insight-only; retrigger the question or adjust prompt/model
- Charts not rendering
  - Ensure SQL returns rows and a suitable numeric/time column; try a simpler question

---

## Extending the System

- Add new metadata queries in `metadata_queries.py` (e.g., tags, masking policies, tasks)
- Enrich doc construction in `save_metadata_docs` (more context, business glossary)
- Enhance graph with FK edges and join hints; surface in RAG context
- Add synonyms/domain terminology to improve retrieval quality
- Swap/augment embedding models or LLMs as needed

---

## Security Notes

- `users.txt` permits plain or SHA256-hashed passwords with `{SHA256}` prefix
- Store Snowflake credentials via environment variables; avoid committing secrets
- Limit the app role to read-only access for safety; the app does not issue DDL/DML beyond optional CSV upload (Admin-only)

---

## License

Internal / demo use. Adapt licensing as needed for your organization.
