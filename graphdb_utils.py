from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

import networkx as nx
from neo4j import GraphDatabase

from config import app_config


def _get(row: Dict[str, Any], key: str) -> Any:
	return row.get(key, row.get(key.upper()))


class GraphDB:
	def __init__(self) -> None:
		self.neo4j_uri = os.getenv("NEO4J_URI")
		self.neo4j_user = os.getenv("NEO4J_USER")
		self.neo4j_password = os.getenv("NEO4J_PASSWORD")
		self.driver = None
		if self.neo4j_uri and self.neo4j_user and self.neo4j_password:
			self.driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
		self.g = nx.DiGraph()

	def close(self) -> None:
		if self.driver:
			self.driver.close()

	# --------- Local Graph persistence (JSON) ---------
	def load_local(self) -> None:
		path = app_config.graph_path
		if os.path.exists(path):
			try:
				with open(path, "r", encoding="utf-8") as f:
					raw = json.load(f)
				self.g = nx.node_link_graph(raw)
			except Exception:
				self.g = nx.DiGraph()

	def save_local(self) -> None:
		os.makedirs(os.path.dirname(app_config.graph_path), exist_ok=True)
		data = nx.node_link_data(self.g)
		with open(app_config.graph_path, "w", encoding="utf-8") as f:
			json.dump(data, f, ensure_ascii=False)

	# --------- Build graph from metadata ---------
	def build_from_metadata(self, tables: List[Dict[str, Any]], columns: List[Dict[str, Any]]) -> None:
		self.g.clear()
		for t in tables:
			db = _get(t, "database_name")
			schema = _get(t, "schema_name")
			table = _get(t, "table_name")
			if not db or not schema or not table:
				continue
			table_id = f"{db}.{schema}.{table}"
			self.g.add_node(db, label="Database", name=db)
			self.g.add_node(f"{db}.{schema}", label="Schema", name=schema)
			self.g.add_node(table_id, label="Table", name=table, row_count=_get(t, "row_count"))
			self.g.add_edge(db, f"{db}.{schema}", type="CONTAINS")
			self.g.add_edge(f"{db}.{schema}", table_id, type="CONTAINS")
		for c in columns:
			db = _get(c, "database_name")
			schema = _get(c, "schema_name")
			table = _get(c, "table_name")
			column = _get(c, "column_name")
			data_type = _get(c, "data_type")
			if not db or not schema or not table or not column:
				continue
			table_id = f"{db}.{schema}.{table}"
			col_id = f"{table_id}.{column}"
			self.g.add_node(col_id, label="Column", name=column, data_type=data_type)
			self.g.add_edge(table_id, col_id, type="HAS_COLUMN")

	# --------- Search helpers ---------
	def search(self, keyword: str, max_results: int = 10) -> List[Tuple[str, Dict[str, Any]]]:
		keyword_lower = keyword.lower()
		matches: List[Tuple[str, Dict[str, Any]]] = []
		for node_id, data in self.g.nodes(data=True):
			name = str(data.get("name", "")).lower()
			if keyword_lower in name or keyword_lower in node_id.lower():
				matches.append((node_id, data))
			if len(matches) >= max_results:
				break
		return matches

	# --------- Neo4j optional upsert ---------
	def upsert_to_neo4j(self) -> None:
		if not self.driver:
			return
		with self.driver.session() as session:
			session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE")
			for node_id, data in self.g.nodes(data=True):
				session.run(
					"MERGE (n:Node {id: $id}) SET n += $props",
					id=node_id,
					props={"label": data.get("label"), **{k: v for k, v in data.items()}},
				)
			for u, v, data in self.g.edges(data=True):
				session.run(
					"MATCH (a:Node {id: $u}), (b:Node {id: $v}) MERGE (a)-[r:REL {type: $type}]->(b)",
					u=u,
					v=v,
					type=data.get("type", "RELATED"),
				)
