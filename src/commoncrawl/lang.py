from typing import Optional
from langdetect import detect, DetectorFactory


DetectorFactory.seed = 0


def detect_language(text: str) -> Optional[str]:
	try:
		return detect(text) if text and text.strip() else None
	except Exception:
		return None


