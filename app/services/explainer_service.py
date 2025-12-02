"""Explain Datadog Log Search queries and log entries."""

import anthropic

from configs.config import get_settings
from configs.logger import get_logger
from services.vectorstore_service import get_rag_context, list_collections
from prompts import LOG_EXPLAINER_SYSTEM_PROMPT, DDSQL_EXPLAINER_SYSTEM_PROMPT, LOG_ANALYZER_SYSTEM_PROMPT

logger = get_logger("explainer_service")


def explain_log_query(query: str) -> str:
    """
    Explain a Datadog Log Search query in plain English.
    
    Args:
        query: The Datadog Log Search query to explain
        
    Returns:
        Human-readable explanation of the query
    """
    logger.info(f"Explaining query: {query[:100]}...")
    
    settings = get_settings()
    
    # Get RAG context from all collections
    rag_context = ""
    collections = list_collections()
    if collections:
        for collection in collections:
            collection_name = collection["name"]
            logger.debug(f"Fetching RAG context from: {collection_name}")
            context = get_rag_context(query, collection_name, limit=3)
            rag_context += context
    else:
        logger.debug("No collections available for RAG context")
    
    logger.debug(f"RAG context length: {len(rag_context)} chars")
    
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    
    logger.debug("Sending explanation request to Claude API (streaming)")
    
    with client.messages.stream(
        model=settings.anthropic_model_name,
        max_tokens=settings.anthropic_max_output_tokens,
        temperature=settings.anthropic_temperature,
        system=LOG_EXPLAINER_SYSTEM_PROMPT.format(rag_context=rag_context),
        messages=[{
            "role": "user",
            "content": f"Explain this Datadog Log Search query:\n\n{query}"
        }]
    ) as stream:
        response = stream.get_final_message()
    
    # Log token usage
    logger.debug(f"Claude response - input tokens: {response.usage.input_tokens}, output tokens: {response.usage.output_tokens}")
    
    explanation = response.content[0].text.strip()
    logger.info(f"Generated explanation ({len(explanation)} chars)")
    
    return explanation


def explain_ddsql_query(query: str) -> str:
    """
    Explain a DDSQL query in plain English.
    
    Args:
        query: The DDSQL query to explain
        
    Returns:
        Human-readable explanation of the query
    """
    logger.info(f"Explaining DDSQL query: {query[:100]}...")
    
    settings = get_settings()
    
    # Get RAG context from all collections
    rag_context = ""
    collections = list_collections()
    if collections:
        for collection in collections:
            collection_name = collection["name"]
            logger.debug(f"Fetching RAG context from: {collection_name}")
            context = get_rag_context(query, collection_name, limit=3)
            rag_context += context
    else:
        logger.debug("No collections available for RAG context")
    
    logger.debug(f"RAG context length: {len(rag_context)} chars")
    
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    
    logger.debug("Sending DDSQL explanation request to Claude API (streaming)")
    
    with client.messages.stream(
        model=settings.anthropic_model_name,
        max_tokens=settings.anthropic_max_output_tokens,
        temperature=settings.anthropic_temperature,
        system=DDSQL_EXPLAINER_SYSTEM_PROMPT.format(rag_context=rag_context),
        messages=[{
            "role": "user",
            "content": f"Explain this DDSQL query:\n\n{query}"
        }]
    ) as stream:
        response = stream.get_final_message()
    
    # Log token usage
    logger.debug(f"Claude response - input tokens: {response.usage.input_tokens}, output tokens: {response.usage.output_tokens}")
    
    explanation = response.content[0].text.strip()
    logger.info(f"Generated DDSQL explanation ({len(explanation)} chars)")
    
    return explanation


def explain_log_entry(log_json: str) -> str:
    """
    Analyze a log entry and explain what happened and potential causes.
    
    Args:
        log_json: The log entry in JSON format
        
    Returns:
        Human-readable analysis of the log entry
    """
    logger.info("Analyzing log entry...")
    
    settings = get_settings()
    
    # Get RAG context from all collections
    rag_context = ""
    collections = list_collections()
    if collections:
        for collection in collections:
            collection_name = collection["name"]
            logger.debug(f"Fetching RAG context from: {collection_name}")
            context = get_rag_context(log_json[:500], collection_name, limit=3)
            rag_context += context
    else:
        logger.debug("No collections available for RAG context")
    
    logger.debug(f"RAG context length: {len(rag_context)} chars")
    
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    
    logger.debug("Sending log analysis request to Claude API (streaming)")
    
    with client.messages.stream(
        model=settings.anthropic_model_name,
        max_tokens=settings.anthropic_max_output_tokens,
        temperature=settings.anthropic_temperature,
        system=LOG_ANALYZER_SYSTEM_PROMPT.format(rag_context=rag_context),
        messages=[{
            "role": "user",
            "content": f"Analyze this log entry and explain what happened:\n\n{log_json}"
        }]
    ) as stream:
        response = stream.get_final_message()
    
    # Log token usage
    logger.debug(f"Claude response - input tokens: {response.usage.input_tokens}, output tokens: {response.usage.output_tokens}")
    
    analysis = response.content[0].text.strip()
    logger.info(f"Generated log analysis ({len(analysis)} chars)")
    
    return analysis

