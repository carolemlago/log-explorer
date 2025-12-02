"""Generate Datadog Log Search and DDSQL queries from natural language."""

import json
import anthropic

from configs.config import get_settings
from configs.logger import get_logger
from services.vectorstore_service import get_rag_context, list_collections
from prompts import TRANSLATOR_SYSTEM_PROMPT, DDSQL_TRANSLATOR_SYSTEM_PROMPT

logger = get_logger("generator_service")


def generate_log_query(natural_language: str) -> dict:
    """
    Generate a Datadog Log Search query from natural language.
    
    Args:
        natural_language: The user's question in plain English
        
    Returns:
        Dict with 'query' and 'explanation', or 'needs_clarification' if ambiguous
    """
    logger.info(f"Generating Log Query: {natural_language[:100]}...")
    
    settings = get_settings()
    
    # Get RAG context from all collections
    rag_context = ""
    collections = list_collections()
    if collections:
        for collection in collections:
            collection_name = collection["name"]
            logger.debug(f"Fetching RAG context from: {collection_name}")
            context = get_rag_context(natural_language, collection_name, limit=3)
            rag_context += context
    else:
        logger.debug("No collections available for RAG context")
    
    logger.debug(f"RAG context length: {len(rag_context)} chars")
    
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    
    logger.debug("Sending request to Claude API (streaming)")
    
    with client.messages.stream(
        model=settings.anthropic_model_name,
        max_tokens=settings.anthropic_max_output_tokens,
        temperature=settings.anthropic_temperature,
        system=TRANSLATOR_SYSTEM_PROMPT.format(rag_context=rag_context),
        messages=[{
            "role": "user",
            "content": f"Translate to Datadog Log Search: {natural_language}"
        }]
    ) as stream:
        response = stream.get_final_message()
    
    # Log token usage
    logger.debug(f"Claude response - input tokens: {response.usage.input_tokens}, output tokens: {response.usage.output_tokens}")
    
    # Parse response
    text = response.content[0].text.strip()
    
    # Handle potential markdown wrapping
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    
    try:
        result = json.loads(text)
        
        if result.get("needs_clarification"):
            logger.info(f"Clarification needed: {result.get('message')}")
        else:
            logger.info(f"Generated query: {result.get('query', '')[:100]}")
        
        return result
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse JSON response: {e}")
        return {
            "query": text,
            "explanation": "Generated query (could not parse structured response)"
        }
    except Exception as e:
        logger.error(f"Generation error: {type(e).__name__}: {e}")
        raise


def generate_ddsql_query(natural_language: str) -> dict:
    """
    Generate a DDSQL query from natural language.
    
    Args:
        natural_language: The user's question in plain English
        
    Returns:
        Dict with 'query' and 'explanation', or 'needs_clarification' if ambiguous
    """
    logger.info(f"Generating DDSQL Query: {natural_language[:100]}...")
    
    settings = get_settings()
    
    # Get RAG context from all collections
    rag_context = ""
    collections = list_collections()
    if collections:
        for collection in collections:
            collection_name = collection["name"]
            logger.debug(f"Fetching RAG context from: {collection_name}")
            context = get_rag_context(natural_language, collection_name, limit=3)
            rag_context += context
    else:
        logger.debug("No collections available for RAG context")
    
    logger.debug(f"RAG context length: {len(rag_context)} chars")
    
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    
    logger.debug("Sending DDSQL request to Claude API (streaming)")
    
    with client.messages.stream(
        model=settings.anthropic_model_name,
        max_tokens=settings.anthropic_max_output_tokens,
        temperature=settings.anthropic_temperature,
        system=DDSQL_TRANSLATOR_SYSTEM_PROMPT.format(rag_context=rag_context),
        messages=[{
            "role": "user",
            "content": f"Translate to DDSQL: {natural_language}"
        }]
    ) as stream:
        response = stream.get_final_message()
    
    # Log token usage
    logger.debug(f"Claude response - input tokens: {response.usage.input_tokens}, output tokens: {response.usage.output_tokens}")
    
    # Parse response
    text = response.content[0].text.strip()
    
    # Handle potential markdown wrapping
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    
    try:
        result = json.loads(text)
        
        if result.get("needs_clarification"):
            logger.info(f"Clarification needed: {result.get('message')}")
        else:
            logger.info(f"Generated DDSQL query: {result.get('query', '')[:100]}")
        
        return result
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse JSON response: {e}")
        return {
            "query": text,
            "explanation": "Generated DDSQL query (could not parse structured response)"
        }
    except Exception as e:
        logger.error(f"DDSQL generation error: {type(e).__name__}: {e}")
        raise

