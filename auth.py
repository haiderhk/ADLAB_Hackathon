from __future__ import annotations

import hashlib
from typing import Dict, Tuple

from config import app_config


def _hash_password(password: str) -> str:
	return hashlib.sha256(password.encode("utf-8")).hexdigest()


def load_users(users_file: str | None = None) -> Dict[str, Tuple[str, str]]:
	users: Dict[str, Tuple[str, str]] = {}
	path = users_file or app_config.users_file
	try:
		with open(path, "r", encoding="utf-8") as f:
			for line in f:
				line = line.strip()
				if not line or line.startswith("#"):
					continue
				# username:password:role
				parts = line.split(":")
				if len(parts) != 3:
					continue
				username, password, role = parts
				users[username] = (password, role)
	except FileNotFoundError:
		pass
	return users


def verify_user(username: str, password: str, users: Dict[str, Tuple[str, str]]) -> Tuple[bool, str | None]:
	if username not in users:
		return False, None
	stored_password, role = users[username]
	# Support either plain text or sha256 hex prefixed with {SHA256}
	if stored_password.startswith("{SHA256}"):
		expected_hash = stored_password[len("{SHA256}"):]
		if _hash_password(password) == expected_hash:
			return True, role
		return False, None
	# Plain text
	if stored_password == password:
		return True, role
	return False, None
