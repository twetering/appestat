"""
OpenAI vision fallback for PLUS receipt photos.

Only used when the local OCR reading of a receipt does not add up. The photo is sent to
OpenAI, so this is the one place where receipt images leave the machine. The model's
reading goes through the exact same arithmetic checks as the local OCR result; a model
answer that doesn't add up to the printed totals is never trusted.

Models are configurable via PLUS_LLM_MODELS (comma separated, tried in order).
"""

import base64
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

from sources.plus_receipt import PlusItem, PlusParseResult, _clean_name, _validate

load_dotenv(Path(__file__).parent.parent / ".env")

DEFAULT_MODELS = "gpt-4o,gpt-4o-mini"

# USD per 1M tokens, for cost reporting only
MODEL_PRICING = {
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4o": {"input": 2.50, "output": 10.00},
}

PROMPT = """Je leest een foto van een kassabon van supermarkt PLUS.
Geef ELKE regel van de bon tussen de kolomkop (Omschrijving / P.st/kg / Bedrag) en "Totaal",
in de volgorde van de bon, met het bedrag uit de kolom "Bedrag" precies zoals gedrukt.

Regeltypes:
- "item": een product. quantity = getal vóór "X" (bv. "2X"), anders 1. amount = regelbedrag.
- "korting": regel "Korting" direct onder een product (amount negatief, bv. -1.00).
- "statiegeld": regel "Statiegeld".
- "subtotaal": regel "Subtotaal".
- "punten": regel "PLUSPunten DIGITAAL" (amount positief, bv. 0.34).
- "actie": regels onder het subtotaal met een negatief bedrag (bv. "1X ACTIE AVOCADO €-0,48").
- "totaal": regel "Totaal".
Sla regels zonder bedrag over (bv. "2X VOETBALPLAATJES", "Inclusief korting: ...", "Klant: ...").
Bij gewogen producten met een aparte regel "0,830 kg X 2,25€/kg": gebruik het bedrag van het product.
Verzin niets: als een bedrag onleesbaar is, gebruik null.
Geef ook "totale_korting" uit de regel "DE TOTALE KORTING IS €x" (null als niet zichtbaar)."""

SCHEMA = {
    "name": "plus_receipt",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["lines", "totale_korting"],
        "properties": {
            "totale_korting": {"type": ["number", "null"]},
            "lines": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["type", "text", "quantity", "amount"],
                    "properties": {
                        "type": {"type": "string", "enum": ["item", "korting", "statiegeld", "subtotaal",
                                                            "punten", "actie", "totaal"]},
                        "text": {"type": "string"},
                        "quantity": {"type": "integer"},
                        "amount": {"type": ["number", "null"]},
                    },
                },
            },
        },
    },
}


def _jpeg_data_url(image_path: str) -> str:
    """The API doesn't take HEIC; convert locally with macOS 'sips' and downscale."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "receipt.jpg"
        subprocess.run(["sips", "-s", "format", "jpeg", "-Z", "2048", str(image_path), "--out", str(out)],
                       check=True, capture_output=True)
        return "data:image/jpeg;base64," + base64.b64encode(out.read_bytes()).decode()


def _to_result(data: Dict) -> PlusParseResult:
    """Map the model's line list onto the same structure the OCR parser produces."""
    result = PlusParseResult(stated_savings=data.get("totale_korting"))
    last_item: Optional[PlusItem] = None
    for line in data.get("lines", []):
        kind, amount, text = line["type"], line.get("amount"), line.get("text", "")
        if kind == "item":
            last_item = PlusItem(raw_name=_clean_name(text), quantity=max(1, line.get("quantity") or 1),
                                 amount=amount or 0.0)
            result.items.append(last_item)
        elif kind == "korting" and amount is not None:
            result.item_discounts += abs(amount)
            if last_item:
                last_item.discount += abs(amount)
        elif kind == "statiegeld" and amount is not None:
            result.deposits += amount
        elif kind == "subtotaal":
            result.subtotal = amount
        elif kind == "punten" and amount is not None:
            result.items.append(PlusItem(raw_name="PLUSPUNTEN DIGITAAL", quantity=line.get("quantity") or 1,
                                         amount=abs(amount), is_fee=True))
        elif kind == "actie" and amount is not None:
            result.actions.append((text, -abs(amount)))
        elif kind == "totaal":
            result.total = amount
    _validate(result)
    return result


def parse_with_llm(image_path: str) -> Tuple[Optional[PlusParseResult], Dict]:
    """
    Try the configured models in order; return the first reading that adds up.
    Returns (best_result_or_None, usage_info). Never raises for API problems.
    """
    usage = {"models": [], "cost_usd": 0.0, "error": None}
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        usage["error"] = "OPENAI_API_KEY niet ingesteld"
        return None, usage

    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        image_url = _jpeg_data_url(image_path)
    except Exception as e:
        usage["error"] = str(e)
        return None, usage

    best: Optional[PlusParseResult] = None
    models: List[str] = [m.strip() for m in os.getenv("PLUS_LLM_MODELS", DEFAULT_MODELS).split(",") if m.strip()]
    for model in models:
        try:
            response = client.chat.completions.create(
                model=model,
                temperature=0,
                response_format={"type": "json_schema", "json_schema": SCHEMA},
                messages=[{"role": "user", "content": [
                    {"type": "text", "text": PROMPT},
                    {"type": "image_url", "image_url": {"url": image_url, "detail": "high"}},
                ]}],
            )
        except Exception as e:
            usage["error"] = f"{model}: {e}"
            continue

        pricing = MODEL_PRICING.get(model)
        if pricing and response.usage:
            usage["cost_usd"] += (response.usage.prompt_tokens * pricing["input"]
                                  + response.usage.completion_tokens * pricing["output"]) / 1_000_000
        usage["models"].append(model)

        result = _to_result(json.loads(response.choices[0].message.content))
        result.notes.insert(0, f"Gelezen door {model}")
        if best is None or result.error < best.error:
            best = result
        if result.valid:
            break
    return best, usage
