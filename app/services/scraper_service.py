"""Web scraping functionality using Firecrawl and LangChain text splitting."""

from firecrawl import Firecrawl
from langchain_text_splitters import RecursiveCharacterTextSplitter

from configs.config import get_settings
from configs.logger import get_logger

logger = get_logger("scraper_service")

# Optimal chunk settings for RAG with documentation
CHUNK_SIZE = 1000  # Characters - good balance for embedding quality
CHUNK_OVERLAP = 200  # 20% overlap ensures context continuity


def get_firecrawl_client() -> Firecrawl:
    """Get Firecrawl client."""
    settings = get_settings()
    return Firecrawl(api_key=settings.firecrawl_api_key)


def scrape_url(url: str) -> dict:
    """
    Scrape a URL and return markdown content.
    
    Args:
        url: The URL to scrape
        
    Returns:
        dict with 'markdown', 'title', 'url'
    """
    logger.info(f"Scraping URL: {url}")
    
    client = get_firecrawl_client()
    
    doc = client.scrape(url, formats=["markdown"])
    
    markdown = doc.markdown or ""
    title = getattr(doc, "title", None) or url
    
    logger.info(f"Scraped {len(markdown)} characters from {url}")
    
    return {
        "markdown": markdown,
        "title": title,
        "url": url,
    }


def chunk_text(text: str) -> list[str]:
    """
    Split text into overlapping chunks using LangChain's RecursiveCharacterTextSplitter.
    
    Uses intelligent splitting that respects document structure:
    - Markdown headers (##, ###)
    - Code blocks
    - Paragraph boundaries
    - Sentence boundaries
    
    Args:
        text: The text to chunk
        
    Returns:
        List of text chunks
    """
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        separators=[
            "\n## ",      # Markdown H2 headers
            "\n### ",     # Markdown H3 headers
            "\n#### ",    # Markdown H4 headers
            "\n```",      # Code block boundaries
            "\n\n",       # Paragraph boundaries
            "\n",         # Line breaks
            ". ",         # Sentence boundaries
            " ",          # Word boundaries
            "",           # Character level (last resort)
        ],
        keep_separator=True,
    )
    
    chunks = splitter.split_text(text)
    
    # Filter empty chunks
    chunks = [chunk.strip() for chunk in chunks if chunk.strip()]
    
    logger.debug(f"Split text into {len(chunks)} chunks (size={CHUNK_SIZE}, overlap={CHUNK_OVERLAP})")
    
    return chunks


def url_to_collection_name(url: str) -> str:
    """
    Generate a collection name from a URL.
    
    Examples:
        https://docs.datadoghq.com/logs/explorer/search_syntax/
        -> docs-datadoghq-logs-explorer-search-syntax
        
        https://example.com/api/v2/users
        -> example-api-v2-users
    
    Args:
        url: The URL to convert
        
    Returns:
        Sanitized collection name
    """
    from urllib.parse import urlparse
    
    parsed = urlparse(url)
    
    # Get domain without www and TLD
    domain = parsed.netloc.replace("www.", "")
    domain_parts = domain.split(".")
    # Keep first part of domain (e.g., "docs" from "docs.datadoghq.com")
    domain_prefix = domain_parts[0] if len(domain_parts) > 2 else domain_parts[0]
    
    # Get path segments
    path = parsed.path.strip("/")
    path_parts = [p for p in path.split("/") if p]
    
    # Combine: domain prefix + path parts (limit to avoid too long names)
    parts = [domain_prefix] + path_parts[:4]
    
    # Sanitize: lowercase, replace special chars with hyphens
    name = "-".join(parts)
    name = name.lower()
    
    # Remove any characters that aren't alphanumeric or hyphens
    import re
    name = re.sub(r'[^a-z0-9-]', '-', name)
    name = re.sub(r'-+', '-', name)  # Collapse multiple hyphens
    name = name.strip('-')
    
    # Limit length
    if len(name) > 50:
        name = name[:50].rstrip('-')
    
    logger.debug(f"Generated collection name '{name}' from URL: {url}")
    
    return name
