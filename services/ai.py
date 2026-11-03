"""
OpenAI access for Appestat: the Decisions API (fixed-choice labelling with probabilities)
and structured JSON completions (free-form normalisation). Both are optional: callers
check available() and fall back to rules when there is no key or the API fails.

Usage is accumulated in USAGE so batch jobs can report what they consumed.
"""

import json
import os
import threading
import time
from pathlib import Path
from typing import Dict, List

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

DECISIONS_URL = "https://api.openai.com/v1/decisions"
CHAT_URL = "https://api.openai.com/v1/chat/completions"
DECISIONS_MODEL = os.getenv("APPESTAT_DECISIONS_MODEL", "gpt-6-luna")
CHAT_MODEL = os.getenv("APPESTAT_CHAT_MODEL", "gpt-5.4-mini")

USAGE = {"decisions_calls": 0, "decisions_tokens": 0, "chat_calls": 0, "chat_input_tokens": 0,
         "chat_output_tokens": 0, "errors": 0}
_lock = threading.Lock()


class AIUnavailable(RuntimeError):
    pass


def available() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


def _post(url: str, payload: Dict, timeout: float = 90, retries: int = 4) -> Dict:
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise AIUnavailable("OPENAI_API_KEY niet ingesteld")
    last = None
    for attempt in range(retries):
        try:
            r = httpx.post(url, headers={"Authorization": f"Bearer {key}"}, json=payload, timeout=timeout)
        except httpx.HTTPError as e:
            last = str(e)
            time.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json()
        last = f"{r.status_code}: {r.text[:300]}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 * (attempt + 1))
            continue
        break
    with _lock:
        USAGE["errors"] += 1
    raise AIUnavailable(last or "onbekende fout")


def decide(text: str, questions: List[Dict], model: str = DECISIONS_MODEL) -> Dict[str, Dict]:
    """
    Ask the Decisions API fixed questions about `text`.
    Returns {question_name: {"value": answer, "confidence": float, "probabilities": [...]}}.
    """
    data = _post(DECISIONS_URL, {"model": model, "input": text, "questions": questions})
    with _lock:
        USAGE["decisions_calls"] += 1
        USAGE["decisions_tokens"] += data.get("usage", {}).get("total_tokens", 0)
    out = {}
    for a in data.get("answers", []):
        if a["type"] == "choice":
            out[a["name"]] = {"value": a.get("choice"), "confidence": a.get("confidence"),
                              "probabilities": a.get("probabilities", [])}
        elif a["type"] == "predicate":
            out[a["name"]] = {"value": a.get("probability"), "confidence": a.get("probability")}
        elif a["type"] == "score":
            out[a["name"]] = {"value": a.get("score"), "confidence": None}
        else:
            out[a["name"]] = {"value": None, "confidence": None}  # refusal
    return out


def json_completion(prompt: str, schema: Dict, model: str = CHAT_MODEL) -> Dict:
    """Chat completion constrained to a JSON schema; returns the parsed object."""
    data = _post(CHAT_URL, {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_schema", "json_schema": schema},
    }, timeout=180)
    usage = data.get("usage", {})
    with _lock:
        USAGE["chat_calls"] += 1
        USAGE["chat_input_tokens"] += usage.get("prompt_tokens", 0)
        USAGE["chat_output_tokens"] += usage.get("completion_tokens", 0)
    return json.loads(data["choices"][0]["message"]["content"])


def usage_report() -> str:
    u = USAGE
    return (f"Decisions: {u['decisions_calls']} calls / {u['decisions_tokens']} tokens · "
            f"LLM: {u['chat_calls']} calls / {u['chat_input_tokens']}+{u['chat_output_tokens']} tokens · "
            f"fouten: {u['errors']}")
