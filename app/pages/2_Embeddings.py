"""
Embeddings - Manage RAG knowledge base with hybrid search.

Scrape URLs to markdown and generate hybrid embeddings:
- Dense: OpenAI text-embedding-3-large
- Sparse: FastEmbed SPLADE
"""

import streamlit as st
import streamlit.components.v1 as components
from services.scraper_service import url_to_collection_name
from configs.logger import get_logger
from renderings import EMBEDDINGS_WORKFLOW_SVG
from services.vectorstore_service import (
    list_collections,
    index_url,
    hybrid_search,
    delete_collection,
)

logger = get_logger("embeddings")

st.set_page_config(
    page_title="Embeddings",
    page_icon="🧠",
    layout="wide",
)

# --- HOW IT WORKS ---
with st.expander("How It Works", expanded=True):
    components.html(EMBEDDINGS_WORKFLOW_SVG, height=300, scrolling=False)

st.divider()

# --- COLLECTIONS ---
st.subheader("Collections")

collections = list_collections()

if collections:
    col1, col2 = st.columns([4, 1])
    
    with col1:
        # Dropdown with collection info
        collection_options = [
            f"{c['name']} ({c['points_count']} chunks)"
            for c in collections
        ]
        selected_option = st.selectbox(
            "Select collection:",
            options=collection_options,
            index=None,
            placeholder="Choose a collection",
            label_visibility="collapsed",
        )
        # Extract collection name from selection
        selected_collection = selected_option.split(" (")[0] if selected_option else None
    
    with col2:
        if st.button("Delete", type="secondary", use_container_width=True):
            if selected_collection:
                delete_collection(selected_collection)
                st.rerun()
else:
    st.info("No collections yet. Add a URL below to create one.")

st.divider()

# --- ADD URL ---
st.subheader("Add URL")

# Initialize session state for collection name
if "generated_collection_name" not in st.session_state:
    st.session_state.generated_collection_name = ""

url_input = st.text_input(
    "URL to scrape:",
    placeholder="https://docs.datadoghq.com/logs/explorer/search_syntax/",
    key="url_input",
)

# Auto-generate collection name when URL changes
if url_input:
    generated_name = url_to_collection_name(url_input)
    if generated_name != st.session_state.generated_collection_name:
        st.session_state.generated_collection_name = generated_name

# Show generated collection name (editable)
collection_name = st.text_input(
    "Collection name (auto-generated):",
    value=st.session_state.generated_collection_name,
    placeholder="Enter URL above to auto-generate",
)

# Progress placeholders
progress_container = st.empty()
status_container = st.empty()

if st.button("Index URL", type="primary", use_container_width=True):
    if url_input and collection_name:
        # Sanitize collection name
        safe_name = collection_name.lower().replace(" ", "-")
        
        # Create progress bar
        progress_bar = progress_container.progress(0)
        
        def update_progress(current: int, total: int, status: str):
            """Callback to update progress bar and status."""
            if total > 0:
                progress_bar.progress(current / total, text=f"{current}/{total} chunks")
            status_container.caption(status)
        
        try:
            chunks_indexed = index_url(
                collection_name=safe_name,
                url=url_input,
                progress_callback=update_progress,
            )
            # Clear progress indicators
            progress_container.empty()
            status_container.empty()
            
            st.success(f"Indexed {chunks_indexed} chunks into '{safe_name}'")
            # Reset session state
            st.session_state.generated_collection_name = ""
            st.rerun()
        except Exception as e:
            # Clear progress indicators
            progress_container.empty()
            status_container.empty()
            
            logger.error(f"Failed to index URL: {e}")
            st.error(f"Failed to index: {e}")
    else:
        st.warning("Please enter a URL.")

st.divider()

# --- SEARCH TEST ---
st.subheader("Test Hybrid Search")

col1, col2 = st.columns([3, 1])

with col1:
    search_query = st.text_input(
        "Search query:",
        placeholder="filter by HTTP status code",
    )

with col2:
    search_collection = st.selectbox(
        "Collection:",
        options=[c["name"] for c in collections] if collections else ["No collections"],
        disabled=not collections,
    )

if st.button("Search", use_container_width=True):
    if search_query and collections:
        with st.spinner("Searching..."):
            results = hybrid_search(search_collection, search_query, limit=5)
        
        if results:
            st.markdown(f"**Found {len(results)} results:**")
            
            for i, result in enumerate(results, 1):
                with st.expander(f"Result {i} (score: {result['score']:.3f})"):
                    st.markdown(result["text"])
                    st.caption(f"Source: {result['url']}")
        else:
            st.info("No results found.")
    elif not collections:
        st.warning("No collections to search. Add a URL first.")

st.divider()

# --- SUGGESTED URLS ---
st.subheader("Suggested URLs")

st.markdown("Click to copy, then paste above:")

suggested = [
    ("Datadog Log Search Syntax", "https://docs.datadoghq.com/logs/explorer/search_syntax/"),
    ("Datadog Log Facets", "https://docs.datadoghq.com/logs/explorer/facets/"),
    ("Datadog Security Signals", "https://docs.datadoghq.com/security/cloud_siem/log_detection_rules/"),
    ("AWS CloudTrail Logs", "https://docs.datadoghq.com/integrations/amazon_cloudtrail/"),
    ("DDSQL Reference", "https://docs.datadoghq.com/ddsql_reference/"),
]

for name, url in suggested:
    st.code(url, language=None)
