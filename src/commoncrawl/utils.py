import os
import yaml
from typing import Dict


def load_yaml(path: str) -> Dict:
	with open(path, "r", encoding="utf-8") as f:
		return yaml.safe_load(f)


def resolve_path(template: str) -> str:
	return os.path.expandvars(os.path.expanduser(template))


