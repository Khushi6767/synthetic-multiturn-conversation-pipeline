import json
import re

def parse_raw(raw: str):
    if not raw:
        return None

    text = raw.strip().replace("\n", " ")
    match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)

    if not match:
        return None

    try:
        obj = json.loads(match.group(0))
        return obj if isinstance(obj, list) else None
    except json.JSONDecodeError:
        return None
