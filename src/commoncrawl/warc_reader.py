from typing import Optional, Tuple, Dict
import io
import hashlib
import requests
from warcio.archiveiterator import ArchiveIterator


def http_range_fetch(base_https: str, warc_filename: str, offset: int, length: int, timeout: int = 60) -> bytes:
	url = base_https.rstrip('/') + '/' + warc_filename
	headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
	r = requests.get(url, headers=headers, timeout=timeout)
	r.raise_for_status()
	return r.content


def parse_single_record(record_bytes: bytes) -> Tuple[Optional[int], Optional[str], Optional[str], Optional[bytes]]:
	"""Returns (status_code, target_uri, content_type, http_payload_bytes)."""
	stream = io.BytesIO(record_bytes)
	for record in ArchiveIterator(stream):
		if record.rec_type != 'response':
			continue
		status_code = None
		try:
			status_code = int(record.http_headers.get_header('Status')) if record.http_headers else None
		except Exception:
			status_code = None
		target_uri = record.rec_headers.get_header('WARC-Target-URI')
		content_type = record.http_headers.get_header('Content-Type') if record.http_headers else None
		payload = record.content_stream().read()
		return status_code, target_uri, content_type, payload
	return None, None, None, None


def sha1_digest(data: bytes) -> str:
	return hashlib.sha1(data).hexdigest()


