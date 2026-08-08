"""LLM client for a local Ollama instance (primary)."""
import requests

from src.llm.base_llm import LLMClient


class OllamaClient(LLMClient):
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3.1:8b") -> None:
        self.base_url = base_url
        self.model = model

    def is_available(self) -> bool:
        """Return True if the Ollama API is reachable."""
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 2048,
    ) -> str:
        """Generate text via the Ollama /api/generate endpoint."""
        try:
            resp = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "system": system_prompt or "",
                    "stream": False,
                    "options": {"temperature": temperature, "num_predict": max_tokens},
                },
                timeout=60,
            )
            return resp.json()["response"]
        except Exception as e:
            raise RuntimeError(f"Ollama generation failed: {e}") from e
