"""Configuration — loads .env and creates the Gemini model client."""

import os
from pathlib import Path

from autogen_core.models import ModelInfo
from autogen_ext.models.openai import OpenAIChatCompletionClient
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_root = Path(__file__).parent.parent.parent
load_dotenv(_root / ".env")


def get_model_client() -> OpenAIChatCompletionClient:
    """Return an OpenAI-compatible client pointed at the Gemini API."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY not set. Copy .env.example → .env and add your key."
        )
    return OpenAIChatCompletionClient(
        model="models/gemini-2.5-flash",
        api_key=api_key,
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        model_info=ModelInfo(
            vision=False,
            function_calling=True,
            json_output=True,
            family="gemini",
            structured_output=True,
        ),
    )
