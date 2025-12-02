"""
Natural Language Log Explorer - Home Page
"""

import streamlit as st
import streamlit.components.v1 as components
from renderings import HERO_SVG

st.set_page_config(
    page_title="Log Explorer",
    page_icon="🧠",
    layout="wide",
)

# Feature Cards HTML
FEATURE_CARDS = '''
<style>
.card-container {
    display: flex;
    gap: 20px;
    padding: 20px 0;
    justify-content: center;
    flex-wrap: wrap;
}
.feature-card {
    background: linear-gradient(145deg, #1f1b2e 0%, #2d2640 100%);
    border-radius: 16px;
    padding: 28px;
    width: 280px;
    border: 1px solid rgba(168, 85, 247, 0.2);
    transition: all 0.3s ease;
    cursor: pointer;
    text-decoration: none;
}
.feature-card:hover {
    transform: translateY(-4px);
    border-color: rgba(168, 85, 247, 0.5);
    box-shadow: 0 12px 40px rgba(168, 85, 247, 0.15);
}
.card-icon {
    font-size: 36px;
    margin-bottom: 16px;
}
.card-title {
    color: #e9d5ff;
    font-size: 20px;
    font-weight: 600;
    margin-bottom: 12px;
    font-family: system-ui, -apple-system, sans-serif;
}
.card-description {
    color: #9ca3af;
    font-size: 14px;
    line-height: 1.6;
    margin-bottom: 16px;
    font-family: system-ui, -apple-system, sans-serif;
}
</style>

<div class="card-container">
    <div class="feature-card">
        <div class="card-icon">✨</div>
        <div class="card-title">Generate</div>
        <div class="card-description">Transform natural language into precise Datadog queries. Supports both Log Search and DDSQL syntax.</div>
    </div>
    
    <div class="feature-card">
        <div class="card-icon">🔍</div>
        <div class="card-title">Search</div>
        <div class="card-description">Execute queries directly against your Datadog instance via the Logs API. Get real results instantly.</div>
    </div>
    
    <div class="feature-card">
        <div class="card-icon">💡</div>
        <div class="card-title">Explain</div>
        <div class="card-description">Decode any query or log entry into plain English with context from your indexed documentation.</div>
    </div>
</div>
'''

# Reduce top padding
st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)

# Integrated hero header
st.markdown("""
<div style="text-align: center; padding: 10px 0 10px 0;">
    <h1 style="color: #f3f4f6; font-size: 42px; font-weight: 700; margin-bottom: 16px; font-family: system-ui, -apple-system, sans-serif;">
        Natural Language Log Explorer
    </h1>
    <p style="color: #9ca3af; font-size: 18px; line-height: 1.7; max-width: 700px; margin: 0 auto;">
        An AI-powered toolkit for <strong style="color: #a78bfa;">Datadog Logs</strong>. Generate queries from plain English, 
        execute searches against your Datadog instance, and get intelligent explanations of complex queries, all enhanced 
        with RAG context from your indexed documentation.
    </p>
</div>
""", unsafe_allow_html=True)

# Render animated hero
components.html(HERO_SVG, height=300)

# Render feature cards
components.html(FEATURE_CARDS, height=380)

