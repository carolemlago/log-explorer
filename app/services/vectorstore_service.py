"""
Vector store for hybrid RAG using Qdrant local mode.

Uses:
- OpenAI text-embedding-3-large for dense embeddings
- FastEmbed SPLADE for sparse embeddings
"""

from pathlib import Path
from typing import Callable

from qdrant_client import QdrantClient, models
from openai import OpenAI
from fastembed import SparseTextEmbedding

from configs.config import get_settings
from configs.logger import get_logger
from services.scraper_service import scrape_url, chunk_text

logger = get_logger("vectorstore_service")

QDRANT_PATH = Path("/data/qdrant")


def get_qdrant_client() -> QdrantClient:
    """Get Qdrant client in local mode."""
    QDRANT_PATH.mkdir(parents=True, exist_ok=True)
    return QdrantClient(path=str(QDRANT_PATH))


def get_openai_client() -> OpenAI:
    """Get OpenAI client."""
    settings = get_settings()
    return OpenAI(api_key=settings.openai_api_key)


def get_sparse_model() -> SparseTextEmbedding:
    """Get FastEmbed sparse model."""
    settings = get_settings()
    return SparseTextEmbedding(model_name=settings.qdrant_sparse_embedding_model)


def create_collection(client: QdrantClient, collection_name: str) -> None:
    """Create a collection with hybrid vector config."""
    settings = get_settings()
    collections = [c.name for c in client.get_collections().collections]
    
    if collection_name in collections:
        logger.debug(f"Collection {collection_name} already exists")
        return
    
    logger.info(f"Creating collection: {collection_name}")
    
    client.create_collection(
        collection_name=collection_name,
        vectors_config={
            "dense": models.VectorParams(
                size=settings.openai_embedding_dimensions,
                distance=models.Distance.COSINE,
            ),
        },
        sparse_vectors_config={
            "sparse": models.SparseVectorParams(
                modifier=models.Modifier.IDF,
            ),
        },
    )


def get_dense_embedding(openai_client: OpenAI, text: str) -> list[float]:
    """Get dense embedding from OpenAI."""
    settings = get_settings()
    response = openai_client.embeddings.create(
        model=settings.openai_embedding_model,
        input=text,
    )
    return response.data[0].embedding


def get_sparse_embedding(sparse_model: SparseTextEmbedding, text: str) -> models.SparseVector:
    """Get sparse embedding from FastEmbed SPLADE."""
    embeddings = list(sparse_model.embed([text]))
    embedding = embeddings[0]
    
    return models.SparseVector(
        indices=embedding.indices.tolist(),
        values=embedding.values.tolist(),
    )


def index_url(
    collection_name: str,
    url: str,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> int:
    """
    Scrape URL and index chunks with hybrid embeddings.
    
    Args:
        collection_name: Name of the Qdrant collection
        url: URL to scrape and index
        progress_callback: Callback(current, total, status) for progress updates
    
    Returns:
        Number of chunks indexed
    """
    def update_progress(current: int, total: int, status: str):
        if progress_callback:
            progress_callback(current, total, status)
    
    qdrant = get_qdrant_client()
    openai_client = get_openai_client()
    sparse_model = get_sparse_model()
    
    update_progress(0, 0, "Creating collection...")
    create_collection(qdrant, collection_name)
    
    # Scrape
    update_progress(0, 0, "Scraping URL...")
    scraped = scrape_url(url)
    markdown = scraped["markdown"]
    
    if not markdown:
        logger.warning(f"No content scraped from {url}")
        return 0
    
    # Chunk using LangChain's intelligent splitter
    update_progress(0, 0, "Chunking text...")
    chunks = chunk_text(markdown)
    total_chunks = len(chunks)
    logger.info(f"Split into {total_chunks} chunks")
    
    # Get current point count for ID offset
    collection_info = qdrant.get_collection(collection_name)
    id_offset = collection_info.points_count
    
    # Index each chunk with progress updates
    points = []
    for i, chunk in enumerate(chunks):
        update_progress(i + 1, total_chunks, f"Embedding chunk {i + 1}/{total_chunks}")
        
        dense = get_dense_embedding(openai_client, chunk)
        sparse = get_sparse_embedding(sparse_model, chunk)
        
        points.append(
            models.PointStruct(
                id=id_offset + i,
                vector={"dense": dense},
                payload={
                    "text": chunk,
                    "url": url,
                    "title": scraped["title"],
                    "chunk_index": i,
                },
            )
        )
        
        # Set sparse vector separately
        points[-1].vector["sparse"] = sparse
    
    # Upsert
    update_progress(total_chunks, total_chunks, "Saving to Qdrant...")
    qdrant.upsert(collection_name=collection_name, points=points)
    logger.info(f"Indexed {len(points)} chunks from {url}")
    
    return len(points)


def hybrid_search(
    collection_name: str,
    query: str,
    limit: int = 5,
) -> list[dict]:
    """
    Perform hybrid search combining dense and sparse results.
    
    Returns:
        List of dicts with 'text', 'url', 'title', 'score'
    """
    qdrant = get_qdrant_client()
    openai_client = get_openai_client()
    sparse_model = get_sparse_model()
    
    # Check collection exists
    collections = [c.name for c in qdrant.get_collections().collections]
    if collection_name not in collections:
        logger.warning(f"Collection {collection_name} does not exist")
        return []
    
    # Get embeddings for query
    dense_query = get_dense_embedding(openai_client, query)
    sparse_query = get_sparse_embedding(sparse_model, query)
    
    # Hybrid search with RRF fusion
    results = qdrant.query_points(
        collection_name=collection_name,
        prefetch=[
            models.Prefetch(
                query=dense_query,
                using="dense",
                limit=limit * 2,
            ),
            models.Prefetch(
                query=sparse_query,
                using="sparse",
                limit=limit * 2,
            ),
        ],
        query=models.FusionQuery(fusion=models.Fusion.RRF),
        limit=limit,
    )
    
    return [
        {
            "text": point.payload.get("text", ""),
            "url": point.payload.get("url", ""),
            "title": point.payload.get("title", ""),
            "score": point.score,
        }
        for point in results.points
    ]


def list_collections() -> list[dict]:
    """List all collections with stats."""
    qdrant = get_qdrant_client()
    collections = []
    
    for collection in qdrant.get_collections().collections:
        info = qdrant.get_collection(collection.name)
        collections.append({
            "name": collection.name,
            "points_count": info.points_count,
            "status": info.status,
        })
    
    return collections


def delete_collection(collection_name: str) -> None:
    """Delete a collection."""
    qdrant = get_qdrant_client()
    qdrant.delete_collection(collection_name)
    logger.info(f"Deleted collection: {collection_name}")


def get_rag_context(query: str, collection_name: str, limit: int = 5) -> str:
    """
    Get RAG context for a query from a specific collection.
    
    Returns:
        Formatted context string for LLM prompt
    """
    results = hybrid_search(collection_name, query, limit=limit)
    
    if not results:
        return ""
    
    context = "\n\nRELEVANT DOCUMENTATION:\n"
    for i, result in enumerate(results, 1):
        context += f"\n{i}. {result['text'][:500]}\n"
        context += f"   Source: {result['url']}\n"
    
    return context

