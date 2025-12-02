"""Configuration settings for the Log Explorer app."""

from pydantic import field_validator
from pydantic_settings import BaseSettings

from configs.logger import get_logger

logger = get_logger("config")


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Anthropic Settings
    anthropic_api_key: str
    anthropic_model_name: str 
    anthropic_temperature: float
    anthropic_max_output_tokens: int

    # OpenAI Settings (embeddings only)
    openai_api_key: str
    openai_embedding_model: str
    openai_embedding_dimensions: int

    # Firecrawl Settings
    firecrawl_api_key: str

    # Datadog Settings
    dd_api_key: str
    dd_app_key: str
    dd_site: str

    # Qdrant Settings
    qdrant_sparse_embedding_model: str

    # App settings
    app_name: str = "Natural Language Log Explorer"
    debug: bool = False

    @field_validator('anthropic_api_key', 'openai_api_key', 'firecrawl_api_key', 'dd_api_key', 'dd_app_key')
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        """Validate that API keys are properly configured."""
        if not v or v.strip() == "":
            raise ValueError("API key cannot be empty")
        if v.startswith('your_'):
            raise ValueError("API key not configured - please update .env file with real API keys")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


_settings: Settings | None = None


def get_settings() -> Settings:
    """Get application settings (singleton pattern)."""
    global _settings
    
    if _settings is None:
        logger.info("Loading application settings from environment")
        _settings = Settings()
        logger.info(f"Settings loaded - Datadog site: {_settings.dd_site}")
    
    return _settings
