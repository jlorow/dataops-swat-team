"""Abstract interface for LLM clients used by DataOps SWAT agents."""
from abc import ABC, abstractmethod


class LLMClient(ABC):
    @abstractmethod
    def generate(self, prompt: str, system_prompt: str | None = None, temperature: float = 0.1) -> str:
        """Generate text from the LLM. Returns the generated string."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the LLM service is reachable."""
        pass
