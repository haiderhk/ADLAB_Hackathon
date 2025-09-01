from __future__ import annotations

import json
import os

from config import app_config
from metadata_extractor import extract_metadata, save_metadata_docs
from rag_pipeline import upsert_docs_to_chroma


def run() -> None:
	md = extract_metadata()
	docs = save_metadata_docs(md)
	os.makedirs("./data", exist_ok=True)
	with open("./data/metadata_docs.json", "w", encoding="utf-8") as f:
		json.dump(docs, f, ensure_ascii=False, indent=2)
	count = upsert_docs_to_chroma(docs)
	print(f"Indexed {count} documents into Chroma at {app_config.chroma_persist_dir}")


if __name__ == "__main__":
	run()
