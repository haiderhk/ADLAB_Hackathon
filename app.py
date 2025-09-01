from __future__ import annotations

import logging
import os
import json
from typing import Dict

import pandas as pd
import streamlit as st

from auth import load_users, verify_user
from config import app_config
from metadata_extractor import extract_metadata, save_metadata_docs
from metadata_extractor import connection_test
from rag_pipeline import generate_sql_and_insight, render_chart, run_sql, upsert_docs_to_chroma

# -------- Logging --------
os.makedirs(os.path.dirname(app_config.logs_path), exist_ok=True)
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
	handlers=[
		logging.FileHandler(app_config.logs_path, encoding="utf-8"),
		logging.StreamHandler(),
	],
)
logger = logging.getLogger("app")


# -------- Auth helpers --------

def login_view() -> Dict[str, str] | None:
	st.title("Insight Agent")
	st.caption("RAG-powered analytics for Snowflake")
	users = load_users()
	with st.form("login_form"):
		username = st.text_input("Username")
		password = st.text_input("Password", type="password")
		submitted = st.form_submit_button("Login")
		if submitted:
			ok, role = verify_user(username, password, users)
			if ok and role:
				st.session_state["user"] = {"username": username, "role": role}
				st.rerun()
			else:
				st.error("Invalid credentials")
	return None


# -------- Pages --------

def page_ask(role: str) -> None:
	st.header("Ask a question")
	question = st.text_input("Ask a business question about your data")
	if st.button("Ask") and question:
		with st.spinner("Thinking..."):
			result = generate_sql_and_insight(question)
		insight = result.get("insight")
		sql = result.get("sql")
		chart_type = result.get("chart_type", "table")
		context = result.get("context", [])

		tabs = st.tabs(["Insight", "SQL", "Chart"]) if role in {"Admin", "Analyst"} else st.tabs(["Insight", "Chart"])

		with tabs[0]:
			st.subheader("Answer")
			st.write(insight)
			st.markdown("**Context used:**")
			with st.expander("Show metadata context"):
				for c in context:
					st.code(c)
		idx = 1
		if role in {"Admin", "Analyst"}:
			with tabs[idx]:
				st.subheader("Generated SQL")
				if sql:
					st.code(sql, language="sql")
				else:
					st.info("No SQL generated.")
			idx += 1
		with tabs[idx]:
			st.subheader("Chart")
			df = pd.DataFrame()
			if sql:
				try:
					df = run_sql(sql)
				except Exception as e:
					st.error(f"SQL execution failed: {e}")
			if df.empty:
				st.info("No data to plot.")
			else:
				fig = render_chart(df, chart_type)
				if fig is not None:
					st.plotly_chart(fig, use_container_width=True)
				with st.expander("Data preview"):
					st.dataframe(df.head(100))
				with st.expander("Quick stats"):
					from rag_pipeline import summarize_dataframe

					for note in summarize_dataframe(df):
						st.write("- ", note)


def metadata_report_section() -> None:
	st.subheader("Knowledge Base Report (Docs powering RAG)")
	docs_path = "./data/metadata_docs.json"
	if not os.path.exists(docs_path):
		st.info("No embedding documents found yet. Click Refresh metadata.")
		return
	try:
		with open(docs_path, "r", encoding="utf-8") as f:
			docs = json.load(f)
	except Exception as e:
		st.error(f"Failed to read docs: {e}")
		return
	st.caption(f"Total docs: {len(docs):,}")
	q = st.text_input("Search docs")
	filtered = docs
	if q:
		q_lower = q.lower()
		filtered = [d for d in docs if q_lower in d.get("text", "").lower() or q_lower in json.dumps(d.get("metadata", {})).lower()]
	st.caption(f"Showing: {len(filtered):,}")
	if filtered:
		df = pd.DataFrame([{**{"id": d.get("id")}, **d.get("metadata", {}), "text": d.get("text")} for d in filtered])
		st.dataframe(df, use_container_width=True, hide_index=True)
		st.download_button("Download filtered docs (JSON)", data=json.dumps(filtered, ensure_ascii=False, indent=2), file_name="metadata_docs_filtered.json")
		st.download_button("Download all docs (JSON)", data=json.dumps(docs, ensure_ascii=False, indent=2), file_name="metadata_docs_all.json")


def page_metadata(role: str) -> None:
	st.header("Metadata report")
	if st.button("Test connection"):
		info = connection_test()
		if info.get("ok"):
			st.success(f"Connected. Context: {info['info']}")
		else:
			st.error(f"Connection failed: {info.get('error')}")

	if role == "Admin":
		if st.button("Refresh metadata"):
			try:
				with st.spinner("Extracting metadata..."):
					md = extract_metadata()
					docs = save_metadata_docs(md)
					count = upsert_docs_to_chroma(docs)
				st.success(f"Metadata refreshed. Docs indexed: {count}")
				st.rerun()
			except Exception as e:
				logger.exception("Refresh failed")
				st.error(f"Refresh failed: {e}")
	else:
		st.info("Only Admin can refresh metadata.")

	st.subheader("Latest Metadata Snapshot")
	path = "./data/metadata_latest.json"
	if os.path.exists(path):
		try:
			with open(path, "r", encoding="utf-8") as f:
				md = json.load(f)
			tables = pd.DataFrame(md.get("tables", []))
			columns = pd.DataFrame(md.get("columns", []))
			colstats = pd.DataFrame(md.get("column_stats", []))
			st.markdown("**Tables**")
			st.dataframe(tables, use_container_width=True, hide_index=True)
			st.markdown("**Columns**")
			st.dataframe(columns, use_container_width=True, hide_index=True)
			st.markdown("**Column Stats (sampled)**")
			st.dataframe(colstats, use_container_width=True, hide_index=True)
		except Exception as e:
			st.error(f"Failed to read snapshot: {e}")
	else:
		st.info("No snapshot found yet. Admin can refresh metadata.")

	with st.expander("Debug"):
		st.caption(f"CWD: {os.getcwd()}")
		st.caption(f"Data dir exists: {os.path.exists('./data')}")
		try:
			st.write(os.listdir('./data'))
		except Exception:
			st.write([])

	metadata_report_section()


def page_admin(role: str) -> None:
	st.header("Admin tools")
	if role != "Admin":
		st.warning("Admins only.")
		return
	st.subheader("Logs")
	try:
		with open(app_config.logs_path, "r", encoding="utf-8") as f:
			lines = f.readlines()[-200:]
		st.code("".join(lines))
	except FileNotFoundError:
		st.info("No logs yet.")

	st.subheader("Upload CSV to Snowflake")
	upload = st.file_uploader("Choose a CSV file", type=["csv"])
	if upload is not None:
		import io
		df = pd.read_csv(io.StringIO(upload.getvalue().decode("utf-8")))
		st.dataframe(df.head())
		dbname = st.text_input("Target Database", value="" if not os.getenv("SNOWFLAKE_DATABASE") else os.getenv("SNOWFLAKE_DATABASE"))
		schema = st.text_input("Target Schema", value="" if not os.getenv("SNOWFLAKE_SCHEMA") else os.getenv("SNOWFLAKE_SCHEMA"))
		table = st.text_input("Target Table Name")
		if st.button("Upload to Snowflake") and table:
			try:
				import snowflake.connector
				from snowflake.connector.pandas_tools import write_pandas
				conn = snowflake.connector.connect(
					account=os.getenv("SNOWFLAKE_ACCOUNT"),
					user=os.getenv("SNOWFLAKE_USER"),
					password=os.getenv("SNOWFLAKE_PASSWORD"),
					warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
					role=os.getenv("SNOWFLAKE_ROLE"),
					database=dbname or os.getenv("SNOWFLAKE_DATABASE"),
					schema=schema or os.getenv("SNOWFLAKE_SCHEMA"),
				)
				with conn:
					ok, nchunks, nrows, _ = write_pandas(conn, df, table_name=table, auto_create_table=True)
				st.success(f"Upload complete: ok={ok}, rows={nrows}")
			except Exception as e:
				st.error(f"Upload failed: {e}")

	st.subheader("Maintenance")
	if st.button("Refresh metadata now"):
		with st.spinner("Refreshing metadata and embeddings..."):
			md = extract_metadata()
			docs = save_metadata_docs(md)
			count = upsert_docs_to_chroma(docs)
		st.success(f"Indexed {count} documents into Chroma.")


# -------- Main --------

def main() -> None:
	st.set_page_config(page_title="Insight Agent", layout="wide")
	user = st.session_state.get("user")

	if not user:
		login_view()
		return

	st.sidebar.write(f"Logged in as: {user['username']} ({user['role']})")
	role = user["role"]
	if role == "Admin":
		pages = ["Ask", "Metadata", "Admin"]
	elif role == "Analyst":
		pages = ["Ask", "Metadata"]
	else:
		pages = ["Ask"]
	page = st.sidebar.radio("Navigation", pages)

	if page == "Ask":
		page_ask(role)
	elif page == "Metadata":
		page_metadata(role)
	elif page == "Admin":
		page_admin(role)


if __name__ == "__main__":
	main()
