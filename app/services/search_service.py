"""Execute Datadog Log Search queries."""

from datetime import datetime, timedelta
import urllib.parse

from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort

from configs.config import get_settings
from configs.logger import get_logger

logger = get_logger("search_service")


def execute_query(
    query: str,
    time_range_minutes: int = 15,
    limit: int = 50
) -> dict:
    """
    Execute a Log Search query against Datadog and return results.
    
    Args:
        query: The Log Search query string
        time_range_minutes: How far back to search (default: 15 minutes)
        limit: Maximum number of logs to return (default: 50)
    
    Returns:
        dict with 'logs', 'count', 'query', and 'datadog_url'
    """
    logger.info(f"Executing query: {query[:100]}...")
    logger.debug(f"Time range: {time_range_minutes} min, limit: {limit}")
    
    settings = get_settings()
    
    configuration = Configuration()
    configuration.api_key["apiKeyAuth"] = settings.dd_api_key
    configuration.api_key["appKeyAuth"] = settings.dd_app_key
    configuration.server_variables["site"] = settings.dd_site
    
    # Calculate time range
    now = datetime.utcnow()
    from_time = now - timedelta(minutes=time_range_minutes)
    
    logger.debug(f"Query time range: {from_time.isoformat()}Z to {now.isoformat()}Z")
    
    # Build the request
    body = LogsListRequest(
        filter=LogsQueryFilter(
            query=query,
            _from=from_time.isoformat() + "Z",
            to=now.isoformat() + "Z",
        ),
        sort=LogsSort.TIMESTAMP_DESCENDING,
        page=LogsListRequestPage(limit=limit),
    )
    
    logs = []
    
    try:
        with ApiClient(configuration) as api_client:
            logs_api = LogsApi(api_client)
            
            logger.debug("Sending request to Datadog Logs API")
            response = logs_api.list_logs(body=body)
            
            for log in response.data:
                attrs = log.attributes
                logs.append({
                    "timestamp": str(attrs.timestamp) if attrs.timestamp else "",
                    "service": attrs.service or "",
                    "status": attrs.status or "",
                    "host": attrs.host or "",
                    "message": attrs.message or "",
                    "attributes": attrs.attributes or {},
                })
        
        logger.info(f"Query returned {len(logs)} logs")
        
        if logs:
            services = set(log["service"] for log in logs if log["service"])
            if services:
                logger.debug(f"Services in results: {', '.join(list(services)[:5])}")
    
    except Exception as e:
        logger.error(f"Datadog Logs API error: {type(e).__name__}: {e}")
        raise
    
    return {
        "logs": logs,
        "count": len(logs),
        "query": query,
        "time_range_minutes": time_range_minutes,
        "datadog_url": build_datadog_url(query, settings.dd_site, time_range_minutes),
    }


def build_datadog_url(query: str, site: str, time_range_minutes: int = 15) -> str:
    """Build a deep link URL to Datadog Log Explorer with the query pre-filled."""
    
    # Determine the base URL
    if site == "datadoghq.com":
        base_url = "https://app.datadoghq.com"
    elif site == "datadoghq.eu":
        base_url = "https://app.datadoghq.eu"
    else:
        base_url = f"https://app.{site}"
    
    # URL encode the query
    encoded_query = urllib.parse.quote(query, safe="")
    
    # Calculate from_ts and to_ts (milliseconds)
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    from_ms = now_ms - (time_range_minutes * 60 * 1000)
    
    return f"{base_url}/logs?query={encoded_query}&from_ts={from_ms}&to_ts={now_ms}"


def format_log_for_display(log: dict) -> dict:
    """Format a log entry for display in the UI."""
    
    # Parse timestamp
    timestamp = log.get("timestamp", "")
    if timestamp:
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, AttributeError):
            pass
    
    return {
        "timestamp": timestamp,
        "service": log.get("service", "—"),
        "status": log.get("status", "—"),
        "host": log.get("host", "—"),
        "message": log.get("message", "")[:200],  # Truncate long messages
    }

