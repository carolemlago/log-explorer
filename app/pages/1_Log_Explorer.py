"""
Log Explorer - Translate, Execute, and Explain Datadog Log Search queries.
"""

import streamlit as st
import streamlit.components.v1 as components
from configs.logger import get_logger
from renderings import GENERATE_WORKFLOW_SVG, LOG_SEARCH_WORKFLOW_SVG, EXPLAIN_WORKFLOW_SVG
from services.generator_service import generate_log_query, generate_ddsql_query
from services.search_service import execute_query
from services.explainer_service import explain_log_query, explain_ddsql_query, explain_log_entry

logger = get_logger("log_explorer")

st.set_page_config(
    page_title="Log Explorer",
    page_icon="🔍",
    layout="wide",
)

tab_generate, tab_log_search, tab_explain = st.tabs(["✨ Generate", "🔍 Log Search", "💡 Explain"])

# --- GENERATE TAB ---
with tab_generate:
    
    with st.expander("How It Works", expanded=True):
        components.html(GENERATE_WORKFLOW_SVG, height=350, scrolling=False)
    
    generate_type = st.radio(
        "Query type:",
        options=["Log Search Query", "DDSQL Query"],
        horizontal=True,
        label_visibility="collapsed",
        key="generate_type_radio"
    )
    
    if generate_type == "Log Search Query":
        user_input = st.text_area(
            "What do you want to find in your logs?",
            placeholder="e.g., Show me failed login attempts from outside our network",
            height=150,
            key="translate_log_search_input",
            label_visibility="collapsed"
        )
        
        if st.button("Generate Query", type="primary", use_container_width=True, key="gen_log_search"):
            if user_input:
                logger.info(f"Translating to Log Search: {user_input[:100]}")
                with st.spinner("Generating Log Query..."):
                    result = generate_log_query(user_input)
                
                if result.get("needs_clarification"):
                    st.warning(result.get("message", "Could you be more specific?"))
                    if result.get("options"):
                        st.markdown("**Did you mean:**")
                        for option in result["options"]:
                            st.markdown(f"• {option}")
                else:
                    st.session_state.last_generated_query = result.get("query", "")
                    logger.info(f"Generated query: {result.get('query', '')}")
                    
                    st.markdown("### Generated Query")
                    st.code(result.get("query", ""), language="bash")

                    if result.get("explanation"):
                        st.info(result["explanation"])
    
    else:  # DDSQL Query
        user_input = st.text_area(
            "What do you want to query?",
            placeholder="e.g., Show me error counts by service for the last hour",
            height=150,
            key="translate_ddsql_input",
            label_visibility="collapsed"
        )
        
        if st.button("Generate Query", type="primary", use_container_width=True, key="gen_ddsql"):
            if user_input:
                logger.info(f"Translating to DDSQL: {user_input[:100]}")
                with st.spinner("Generating DDSQL Query..."):
                    result = generate_ddsql_query(user_input)
                
                if result.get("needs_clarification"):
                    st.warning(result.get("message", "Could you be more specific?"))
                    if result.get("options"):
                        st.markdown("**Did you mean:**")
                        for option in result["options"]:
                            st.markdown(f"• {option}")
                else:
                    st.session_state.last_generated_query = result.get("query", "")
                    logger.info(f"Generated DDSQL query: {result.get('query', '')}")
                    
                    st.markdown("### Generated Query")
                    st.code(result.get("query", ""), language="sql")
                    
                    if result.get("explanation"):
                        st.info(result["explanation"])

# --- LOG SEARCH TAB ---
with tab_log_search:
    
    with st.expander("How It Works", expanded=True):
        components.html(LOG_SEARCH_WORKFLOW_SVG, height=350, scrolling=False)
    
    default_query = st.session_state.get("last_generated_query", "")
    
    query_to_execute = st.text_area(
        "Enter a Log Search query to execute:",
        value=default_query,
        placeholder="e.g., service:payment-service status:error",
        height=150,
        key="execute_input",
        label_visibility="collapsed"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        time_range = st.selectbox(
            "Time range",
            options=[15, 1440, 43200],
            format_func=lambda x: {15: "15 min", 1440: "24 hours", 43200: "30 days"}[x],
            index=0
        )
    
    with col2:
        limit = st.selectbox(
            "Max results",
            options=[10, 25, 50, 100],
            index=2
        )
    
    st.divider()
    
    if st.button("Search", type="primary", use_container_width=True):
        if query_to_execute:
            logger.info(f"Executing Log Search query: {query_to_execute}")
            with st.spinner("Executing query..."):
                result = execute_query(query_to_execute, time_range_minutes=time_range, limit=limit)
            
            logger.info(f"Query returned {result.get('count', 0)} results")
            st.json(result)

# --- EXPLAIN TAB ---
with tab_explain:
    
    with st.expander("How It Works", expanded=True):
        components.html(EXPLAIN_WORKFLOW_SVG, height=500, scrolling=False)
    
    explain_mode = st.radio(
        "What do you want to explain?",
        options=["Log Search Query", "DDSQL Query", "Raw Log"],
        horizontal=True,
        label_visibility="collapsed",
        key="explain_mode_radio"
    )
    
    if explain_mode == "Log Search Query":
        query_input = st.text_area(
            "Paste a Datadog Log Search query to explain:",
            placeholder="e.g., @evt.name:authentication @evt.outcome:failure NOT @network.client.ip:10.*",
            height=150,
            key="explain_log_search_input",
            label_visibility="collapsed"
        )
        
        if st.button("Explain Query", type="primary", use_container_width=True, key="explain_log_search_btn"):
            if query_input:
                logger.info(f"Explaining Log Search query: {query_input}")
                with st.spinner("Analyzing query..."):
                    explanation = explain_log_query(query_input)
                
                st.info(explanation)
    
    elif explain_mode == "DDSQL Query":
        ddsql_input = st.text_area(
            "Paste a DDSQL query to explain:",
            placeholder="e.g., SELECT service, COUNT(*) FROM dd.logs(filter => 'status:error', columns => ARRAY['service']) AS (service VARCHAR) GROUP BY service",
            height=150,
            key="explain_ddsql_input",
            label_visibility="collapsed"
        )
        
        if st.button("Explain Query", type="primary", use_container_width=True, key="explain_ddsql_btn"):
            if ddsql_input:
                logger.info(f"Explaining DDSQL query: {ddsql_input}")
                with st.spinner("Analyzing DDSQL query..."):
                    explanation = explain_ddsql_query(ddsql_input)
                
                st.info(explanation)
    
    else:  # Raw Log
        log_input = st.text_area(
            "Paste a log entry (JSON) to analyze:",
            placeholder='{"timestamp": "...", "service": "auth-service", "status": "error", "message": "...", "attributes": {...}}',
            height=200,
            key="explain_log_input",
            label_visibility="collapsed"
        )
        
        if st.button("Analyze Log", type="primary", use_container_width=True, key="analyze_log_btn"):
            if log_input:
                logger.info(f"Analyzing log entry")
                with st.spinner("Analyzing log..."):
                    explanation = explain_log_entry(log_input)
                
                st.info(explanation)
