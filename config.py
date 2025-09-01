import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SnowflakeConfig:
	account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
	user: str = os.getenv("SNOWFLAKE_USER", "")
	password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
	warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
	database: str = os.getenv("SNOWFLAKE_DATABASE", "")
	role: str = os.getenv("SNOWFLAKE_ROLE", "")
	schema: str | None = os.getenv("SNOWFLAKE_SCHEMA")


@dataclass
class AppConfig:
	chroma_persist_dir: str = os.getenv("CHROMA_DIR", "./chroma_db")
	graph_path: str = os.getenv("GRAPH_PATH", "./data/graphdb.json")
	users_file: str = os.getenv("USERS_FILE", "./users.txt")
	logs_path: str = os.getenv("LOG_FILE", "./logs/app.log")
	openai_api_key: str | None = os.getenv("OPENAI_API_KEY")
	openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
	openai_embedding_model: str = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large")
	default_top_k: int = int(os.getenv("TOP_K", "8"))


snowflake_config = SnowflakeConfig()
app_config = AppConfig()
