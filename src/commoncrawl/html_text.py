from typing import Dict, List, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import justext


def extract_title_text_links(html_bytes: bytes, base_url: str, lang_stoplist: str = "English") -> Tuple[str, str, List[str]]:
	"""Return (title, main_text, outlinks)."""
	soup = BeautifulSoup(html_bytes, "lxml")
	title = (soup.title.string or "").strip() if soup.title else ""
	# jusText for main content
	paras = justext.justext(html_bytes, justext.get_stoplist(lang_stoplist))
	main_text = "\n".join(p.text for p in paras if not p.is_boilerplate)
	# Outlinks
	outlinks: List[str] = []
	for a in soup.find_all("a", href=True):
		href = a.get("href")
		if not href:
			continue
		abs_url = urljoin(base_url, href)
		outlinks.append(abs_url)
	return title, main_text, outlinks


