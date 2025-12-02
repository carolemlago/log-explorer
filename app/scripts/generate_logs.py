"""
Comprehensive Log Generator for Datadog Testing.

Generates realistic, correlated logs across multiple services and infrastructure components.
Includes: security events, APM traces, infrastructure logs, cloud events, and more.

Features:
- Correlated transaction IDs across services
- Realistic time-based patterns (peak hours, incidents)
- Geographic distribution with realistic IP data
- Comprehensive error scenarios and stack traces
- Multiple cloud providers (AWS, GCP, Azure)
- Service mesh and container orchestration logs
- Security and compliance event logging

Usage:
    python generate_logs.py
    python generate_logs.py --count 1000
    python generate_logs.py --count 500 --scenario incident
    python generate_logs.py --duration 60  # Generate for 60 seconds
"""

import argparse
import hashlib
import json
import os
import random
import string
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Optional
import requests

# =============================================================================
# CONFIGURATION
# =============================================================================

DD_API_KEY = os.getenv("DD_API_KEY")
DD_SITE = os.getenv("DD_SITE")

# =============================================================================
# DATA CONSTANTS
# =============================================================================

# Services - Microservices Architecture
SERVICES = {
    "frontend": {
        "api-gateway": {"port": 8080, "language": "go", "framework": "gin"},
        "web-frontend": {"port": 3000, "language": "typescript", "framework": "nextjs"},
        "mobile-bff": {"port": 8081, "language": "kotlin", "framework": "ktor"},
        "graphql-gateway": {"port": 4000, "language": "typescript", "framework": "apollo"},
    },
    "core": {
        "user-service": {"port": 8001, "language": "python", "framework": "fastapi"},
        "auth-service": {"port": 8002, "language": "go", "framework": "gin"},
        "session-service": {"port": 8003, "language": "rust", "framework": "actix"},
        "notification-service": {"port": 8004, "language": "python", "framework": "celery"},
        "email-service": {"port": 8005, "language": "python", "framework": "fastapi"},
    },
    "commerce": {
        "payment-service": {"port": 8010, "language": "java", "framework": "spring"},
        "checkout-service": {"port": 8011, "language": "java", "framework": "spring"},
        "order-service": {"port": 8012, "language": "java", "framework": "spring"},
        "inventory-service": {"port": 8013, "language": "go", "framework": "gin"},
        "pricing-service": {"port": 8014, "language": "python", "framework": "fastapi"},
        "cart-service": {"port": 8015, "language": "node", "framework": "express"},
        "shipping-service": {"port": 8016, "language": "go", "framework": "gin"},
        "tax-service": {"port": 8017, "language": "java", "framework": "spring"},
    },
    "data": {
        "search-service": {"port": 8020, "language": "java", "framework": "spring"},
        "recommendation-service": {"port": 8021, "language": "python", "framework": "fastapi"},
        "analytics-service": {"port": 8022, "language": "python", "framework": "flask"},
        "ml-inference": {"port": 8023, "language": "python", "framework": "fastapi"},
        "etl-service": {"port": 8024, "language": "python", "framework": "airflow"},
        "reporting-service": {"port": 8025, "language": "python", "framework": "fastapi"},
    },
    "infrastructure": {
        "config-service": {"port": 8030, "language": "java", "framework": "spring"},
        "discovery-service": {"port": 8031, "language": "java", "framework": "spring"},
        "vault-proxy": {"port": 8032, "language": "go", "framework": "stdlib"},
    },
}

FLAT_SERVICES = {
    name: info 
    for category in SERVICES.values() 
    for name, info in category.items()
}

ENVIRONMENTS = ["production", "staging", "development", "sandbox"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-northeast-1"]
AVAILABILITY_ZONES = ["a", "b", "c"]

# Hosts and Infrastructure
HOSTS = {
    "web": [f"web-{region}-{i:02d}" for region in ["use1", "usw2", "euw1"] for i in range(1, 6)],
    "api": [f"api-{region}-{i:02d}" for region in ["use1", "usw2", "euw1"] for i in range(1, 8)],
    "worker": [f"worker-{region}-{i:02d}" for region in ["use1", "usw2"] for i in range(1, 5)],
    "db": [f"db-{region}-{i:02d}" for region in ["use1", "usw2"] for i in range(1, 4)],
    "cache": [f"cache-{region}-{i:02d}" for region in ["use1", "usw2"] for i in range(1, 3)],
    "queue": [f"queue-{region}-{i:02d}" for region in ["use1"] for i in range(1, 3)],
}

ALL_HOSTS = [host for hosts in HOSTS.values() for host in hosts]

# Kubernetes
K8S_CLUSTERS = ["prod-us-east", "prod-us-west", "prod-eu", "staging-us"]
K8S_NAMESPACES = ["default", "production", "staging", "monitoring", "logging", "istio-system", "cert-manager"]
K8S_NODE_POOLS = ["general", "compute-optimized", "memory-optimized", "gpu"]

# Databases
DATABASES = {
    "postgresql": {
        "hosts": ["pg-primary-01", "pg-replica-01", "pg-replica-02"],
        "databases": ["users_db", "orders_db", "products_db", "analytics_db"],
        "port": 5432,
    },
    "mysql": {
        "hosts": ["mysql-primary-01", "mysql-replica-01"],
        "databases": ["legacy_app", "reporting"],
        "port": 3306,
    },
    "mongodb": {
        "hosts": ["mongo-01", "mongo-02", "mongo-03"],
        "databases": ["sessions", "user_preferences", "notifications"],
        "port": 27017,
    },
    "elasticsearch": {
        "hosts": ["es-master-01", "es-data-01", "es-data-02", "es-data-03"],
        "indices": ["logs-*", "metrics-*", "traces-*", "products", "search_content"],
        "port": 9200,
    },
    "redis": {
        "hosts": ["redis-master-01", "redis-replica-01"],
        "databases": ["cache", "sessions", "rate_limits"],
        "port": 6379,
    },
    "cassandra": {
        "hosts": ["cassandra-01", "cassandra-02", "cassandra-03"],
        "keyspaces": ["events", "timeseries", "audit"],
        "port": 9042,
    },
}

# Message Queues
MESSAGE_QUEUES = {
    "kafka": {
        "brokers": ["kafka-01", "kafka-02", "kafka-03"],
        "topics": [
            "orders.created", "orders.updated", "orders.completed",
            "payments.processed", "payments.failed",
            "users.registered", "users.updated",
            "inventory.updated", "inventory.low_stock",
            "notifications.email", "notifications.push", "notifications.sms",
            "analytics.events", "analytics.pageviews",
        ],
    },
    "rabbitmq": {
        "hosts": ["rabbitmq-01", "rabbitmq-02"],
        "queues": [
            "email_queue", "sms_queue", "push_notification_queue",
            "order_processing", "payment_retry", "report_generation",
        ],
    },
    "sqs": {
        "queues": [
            "prod-order-processing", "prod-email-delivery", "prod-webhook-delivery",
            "prod-dead-letter", "prod-batch-jobs",
        ],
    },
}

# Cloud Resources - AWS
AWS_RESOURCES = {
    "s3_buckets": [
        "prod-user-uploads", "prod-static-assets", "prod-logs-archive",
        "prod-backups", "prod-ml-models", "prod-data-lake",
        "staging-user-uploads", "dev-sandbox",
    ],
    "lambda_functions": [
        "image-resizer", "thumbnail-generator", "email-sender",
        "order-processor", "inventory-checker", "report-generator",
        "data-transformer", "webhook-handler", "cleanup-job",
    ],
    "rds_instances": [
        "prod-users-primary", "prod-users-replica", "prod-orders-primary",
        "staging-main", "analytics-warehouse",
    ],
    "ec2_instance_types": [
        "t3.micro", "t3.small", "t3.medium", "t3.large",
        "m5.large", "m5.xlarge", "m5.2xlarge",
        "c5.large", "c5.xlarge", "r5.large", "r5.xlarge",
    ],
    "elb": ["prod-api-alb", "prod-web-alb", "staging-alb", "internal-nlb"],
    "cloudfront_distributions": ["E1234567890ABC", "E0987654321DEF"],
}

# Cloud Resources - GCP
GCP_RESOURCES = {
    "gcs_buckets": ["prod-data-backup", "ml-training-data", "analytics-exports"],
    "cloud_functions": ["data-processor", "pubsub-handler", "scheduler-trigger"],
    "cloud_run_services": ["api-service", "worker-service"],
    "bigquery_datasets": ["analytics", "data_warehouse", "ml_features"],
}

# Cloud Resources - Azure
AZURE_RESOURCES = {
    "storage_accounts": ["proddata001", "backups002", "logs003"],
    "app_services": ["api-app-service", "web-app-service"],
    "cosmos_db": ["user-data", "session-store"],
}

# Network Data
NETWORK = {
    "internal_ranges": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"],
    "load_balancers": ["lb-prod-01", "lb-prod-02", "lb-staging-01"],
    "vpn_gateways": ["vpn-office-nyc", "vpn-office-sfo", "vpn-office-lon"],
    "cdn_pops": ["JFK", "LAX", "LHR", "FRA", "NRT", "SIN", "SYD"],
}

# Realistic IP Ranges by Type
IP_POOLS = {
    "internal": [
        (f"10.{a}.{b}.{c}", "Internal")
        for a in range(0, 10) for b in range(0, 5) for c in range(1, 255, 50)
    ][:50],
    "office": [
        ("203.0.113.10", "NYC Office"),
        ("203.0.113.20", "SFO Office"),
        ("203.0.113.30", "London Office"),
        ("203.0.113.40", "Berlin Office"),
    ],
    "cloud": [
        ("52.94.76.0", "AWS us-east-1"),
        ("35.180.0.0", "AWS eu-west-3"),
        ("34.102.136.0", "GCP us-central1"),
        ("20.42.0.0", "Azure eastus"),
    ],
    "residential": [
        (f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}", country)
        for country in ["United States", "Canada", "United Kingdom", "Germany", "France", 
                       "Japan", "Australia", "Brazil", "India", "Singapore"] * 5
    ][:50],
    "suspicious": [
        ("185.220.101.1", "Russia"),
        ("5.188.62.1", "Russia"),
        ("116.31.116.1", "China"),
        ("222.186.180.1", "China"),
        ("175.45.176.1", "North Korea"),
        ("5.34.180.1", "Iran"),
        ("93.184.216.1", "Unknown VPN"),
        ("198.51.100.1", "Tor Exit Node"),
    ],
}

# Users
USERS = {
    "admins": [
        {"id": "u_admin_001", "email": "admin@company.com", "name": "System Admin", "role": "admin"},
        {"id": "u_admin_002", "email": "security@company.com", "name": "Security Admin", "role": "security_admin"},
        {"id": "u_admin_003", "email": "devops@company.com", "name": "DevOps Admin", "role": "admin"},
    ],
    "developers": [
        {"id": "u_dev_001", "email": "alice.chen@company.com", "name": "Alice Chen", "role": "developer"},
        {"id": "u_dev_002", "email": "bob.smith@company.com", "name": "Bob Smith", "role": "developer"},
        {"id": "u_dev_003", "email": "carol.jones@company.com", "name": "Carol Jones", "role": "senior_developer"},
        {"id": "u_dev_004", "email": "david.kim@company.com", "name": "David Kim", "role": "developer"},
        {"id": "u_dev_005", "email": "emma.wilson@company.com", "name": "Emma Wilson", "role": "tech_lead"},
    ],
    "service_accounts": [
        {"id": "sa_deploy", "email": "deploy-bot@company.com", "name": "Deploy Bot", "role": "service"},
        {"id": "sa_monitoring", "email": "monitoring@company.com", "name": "Monitoring Service", "role": "service"},
        {"id": "sa_backup", "email": "backup-service@company.com", "name": "Backup Service", "role": "service"},
        {"id": "sa_ci", "email": "ci-runner@company.com", "name": "CI Runner", "role": "service"},
    ],
    "customers": [
        {"id": f"c_{i:06d}", "email": f"customer{i}@example.com", "name": f"Customer {i}", "role": "customer"}
        for i in range(1, 101)
    ],
    "suspicious": [
        {"id": "u_unknown", "email": "unknown@suspicious.ru", "name": "Unknown", "role": "unknown"},
        {"id": "u_attacker", "email": "h4ck3r@evil.com", "name": "Attacker", "role": "unknown"},
    ],
}

ALL_USERS = [
    user for category in USERS.values() for user in category
]

# HTTP Endpoints
API_ENDPOINTS = {
    "auth": [
        {"path": "/api/v1/auth/login", "method": "POST", "auth_required": False},
        {"path": "/api/v1/auth/logout", "method": "POST", "auth_required": True},
        {"path": "/api/v1/auth/refresh", "method": "POST", "auth_required": True},
        {"path": "/api/v1/auth/register", "method": "POST", "auth_required": False},
        {"path": "/api/v1/auth/forgot-password", "method": "POST", "auth_required": False},
        {"path": "/api/v1/auth/reset-password", "method": "POST", "auth_required": False},
        {"path": "/api/v1/auth/verify-email", "method": "GET", "auth_required": False},
        {"path": "/api/v1/auth/mfa/setup", "method": "POST", "auth_required": True},
        {"path": "/api/v1/auth/mfa/verify", "method": "POST", "auth_required": True},
        {"path": "/api/v1/auth/oauth/google", "method": "GET", "auth_required": False},
        {"path": "/api/v1/auth/oauth/github", "method": "GET", "auth_required": False},
    ],
    "users": [
        {"path": "/api/v1/users", "method": "GET", "auth_required": True},
        {"path": "/api/v1/users/{id}", "method": "GET", "auth_required": True},
        {"path": "/api/v1/users/{id}", "method": "PUT", "auth_required": True},
        {"path": "/api/v1/users/{id}", "method": "DELETE", "auth_required": True},
        {"path": "/api/v1/users/{id}/preferences", "method": "GET", "auth_required": True},
        {"path": "/api/v1/users/{id}/preferences", "method": "PUT", "auth_required": True},
        {"path": "/api/v1/users/{id}/avatar", "method": "POST", "auth_required": True},
        {"path": "/api/v1/users/me", "method": "GET", "auth_required": True},
        {"path": "/api/v1/users/search", "method": "GET", "auth_required": True},
    ],
    "products": [
        {"path": "/api/v1/products", "method": "GET", "auth_required": False},
        {"path": "/api/v1/products/{id}", "method": "GET", "auth_required": False},
        {"path": "/api/v1/products", "method": "POST", "auth_required": True},
        {"path": "/api/v1/products/{id}", "method": "PUT", "auth_required": True},
        {"path": "/api/v1/products/{id}", "method": "DELETE", "auth_required": True},
        {"path": "/api/v1/products/search", "method": "GET", "auth_required": False},
        {"path": "/api/v1/products/categories", "method": "GET", "auth_required": False},
        {"path": "/api/v1/products/{id}/reviews", "method": "GET", "auth_required": False},
        {"path": "/api/v1/products/{id}/reviews", "method": "POST", "auth_required": True},
        {"path": "/api/v1/products/{id}/inventory", "method": "GET", "auth_required": True},
    ],
    "orders": [
        {"path": "/api/v1/orders", "method": "GET", "auth_required": True},
        {"path": "/api/v1/orders/{id}", "method": "GET", "auth_required": True},
        {"path": "/api/v1/orders", "method": "POST", "auth_required": True},
        {"path": "/api/v1/orders/{id}/cancel", "method": "POST", "auth_required": True},
        {"path": "/api/v1/orders/{id}/refund", "method": "POST", "auth_required": True},
        {"path": "/api/v1/orders/{id}/shipping", "method": "GET", "auth_required": True},
        {"path": "/api/v1/orders/{id}/invoice", "method": "GET", "auth_required": True},
    ],
    "payments": [
        {"path": "/api/v1/payments", "method": "POST", "auth_required": True},
        {"path": "/api/v1/payments/{id}", "method": "GET", "auth_required": True},
        {"path": "/api/v1/payments/{id}/refund", "method": "POST", "auth_required": True},
        {"path": "/api/v1/payments/methods", "method": "GET", "auth_required": True},
        {"path": "/api/v1/payments/methods", "method": "POST", "auth_required": True},
        {"path": "/api/v1/payments/webhook", "method": "POST", "auth_required": False},
    ],
    "cart": [
        {"path": "/api/v1/cart", "method": "GET", "auth_required": True},
        {"path": "/api/v1/cart/items", "method": "POST", "auth_required": True},
        {"path": "/api/v1/cart/items/{id}", "method": "PUT", "auth_required": True},
        {"path": "/api/v1/cart/items/{id}", "method": "DELETE", "auth_required": True},
        {"path": "/api/v1/cart/checkout", "method": "POST", "auth_required": True},
        {"path": "/api/v1/cart/apply-coupon", "method": "POST", "auth_required": True},
    ],
    "search": [
        {"path": "/api/v1/search", "method": "GET", "auth_required": False},
        {"path": "/api/v1/search/suggest", "method": "GET", "auth_required": False},
        {"path": "/api/v1/search/filters", "method": "GET", "auth_required": False},
    ],
    "admin": [
        {"path": "/api/v1/admin/users", "method": "GET", "auth_required": True},
        {"path": "/api/v1/admin/users/{id}/suspend", "method": "POST", "auth_required": True},
        {"path": "/api/v1/admin/users/{id}/roles", "method": "PUT", "auth_required": True},
        {"path": "/api/v1/admin/reports", "method": "GET", "auth_required": True},
        {"path": "/api/v1/admin/settings", "method": "GET", "auth_required": True},
        {"path": "/api/v1/admin/settings", "method": "PUT", "auth_required": True},
        {"path": "/api/v1/admin/audit-log", "method": "GET", "auth_required": True},
    ],
    "internal": [
        {"path": "/health", "method": "GET", "auth_required": False},
        {"path": "/ready", "method": "GET", "auth_required": False},
        {"path": "/metrics", "method": "GET", "auth_required": False},
        {"path": "/internal/cache/flush", "method": "POST", "auth_required": True},
        {"path": "/internal/config/reload", "method": "POST", "auth_required": True},
    ],
}

ALL_ENDPOINTS = [
    endpoint for category in API_ENDPOINTS.values() for endpoint in category
]

# User Agents
USER_AGENTS = {
    "browsers": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    ],
    "mobile": [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    ],
    "bots": [
        "Googlebot/2.1 (+http://www.google.com/bot.html)",
        "Bingbot/2.0 (+http://www.bing.com/bingbot.htm)",
        "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
        "Twitterbot/1.0",
        "facebookexternalhit/1.1",
    ],
    "api_clients": [
        "python-requests/2.31.0",
        "axios/1.6.2",
        "curl/8.4.0",
        "Go-http-client/2.0",
        "okhttp/4.12.0",
        "PostmanRuntime/7.35.0",
    ],
    "suspicious": [
        "sqlmap/1.7",
        "nikto/2.1.6",
        "nmap scripting engine",
        "python-urllib3/2.0",
        "",  # Empty user agent
        "-",
    ],
}

# Error Templates
ERROR_TEMPLATES = {
    "python": {
        "exceptions": [
            ("ValueError", "invalid literal for int() with base 10: 'abc'"),
            ("KeyError", "'user_id'"),
            ("TypeError", "'NoneType' object is not subscriptable"),
            ("AttributeError", "'dict' object has no attribute 'items'"),
            ("ConnectionError", "Connection refused"),
            ("TimeoutError", "Connection timed out after 30 seconds"),
            ("JSONDecodeError", "Expecting value: line 1 column 1 (char 0)"),
            ("ValidationError", "field required: email"),
            ("IntegrityError", "duplicate key value violates unique constraint"),
            ("OperationalError", "connection to server lost"),
        ],
        "stack_template": """Traceback (most recent call last):
  File "/app/{service}/main.py", line {line1}, in {func1}
    result = {operation1}
  File "/app/{service}/handlers/{handler}.py", line {line2}, in {func2}
    data = {operation2}
  File "/app/lib/utils.py", line {line3}, in {func3}
    return {operation3}
{exception}: {message}""",
    },
    "java": {
        "exceptions": [
            ("NullPointerException", "Cannot invoke method on null object"),
            ("IllegalArgumentException", "Invalid parameter value"),
            ("SQLException", "Connection pool exhausted"),
            ("IOException", "Connection reset by peer"),
            ("TimeoutException", "Request timed out"),
            ("OutOfMemoryError", "Java heap space"),
            ("StackOverflowError", "Recursive call depth exceeded"),
            ("ConcurrentModificationException", "Collection modified during iteration"),
            ("NoSuchElementException", "No value present"),
            ("OptimisticLockException", "Row was updated by another transaction"),
        ],
        "stack_template": """java.lang.{exception}: {message}
\tat com.company.{service}.{class1}.{method1}({class1}.java:{line1})
\tat com.company.{service}.{class2}.{method2}({class2}.java:{line2})
\tat com.company.common.{class3}.{method3}({class3}.java:{line3})
\tat org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:897)
\tat javax.servlet.http.HttpServlet.service(HttpServlet.java:750)""",
    },
    "go": {
        "exceptions": [
            ("panic", "runtime error: index out of range"),
            ("error", "connection refused"),
            ("error", "context deadline exceeded"),
            ("error", "invalid memory address or nil pointer dereference"),
            ("error", "sql: no rows in result set"),
        ],
        "stack_template": """{exception}: {message}
goroutine 1 [running]:
main.{func1}(...)
\t/app/{service}/main.go:{line1}
{service}/{package}.{func2}(0x0, 0x0)
\t/app/{service}/{package}/{file}.go:{line2} +0x{offset}
runtime.main()
\t/usr/local/go/src/runtime/proc.go:250 +0x1c9""",
    },
    "node": {
        "exceptions": [
            ("TypeError", "Cannot read property 'id' of undefined"),
            ("ReferenceError", "user is not defined"),
            ("SyntaxError", "Unexpected token in JSON"),
            ("Error", "ECONNREFUSED"),
            ("Error", "ETIMEDOUT"),
            ("RangeError", "Maximum call stack size exceeded"),
        ],
        "stack_template": """{exception}: {message}
    at {func1} (/app/{service}/src/{file1}.js:{line1}:{col1})
    at {func2} (/app/{service}/src/{file2}.js:{line2}:{col2})
    at processTicksAndRejections (internal/process/task_queues.js:95:5)
    at async {func3} (/app/{service}/src/{file3}.js:{line3}:{col3})""",
    },
}

# Security Events
SECURITY_EVENTS = {
    "authentication": [
        {"event": "login_success", "severity": "info", "message": "User logged in successfully"},
        {"event": "login_failed", "severity": "warn", "message": "Failed login attempt"},
        {"event": "login_blocked", "severity": "warn", "message": "Login blocked due to rate limiting"},
        {"event": "account_locked", "severity": "warn", "message": "Account locked after multiple failed attempts"},
        {"event": "password_changed", "severity": "info", "message": "User password changed"},
        {"event": "mfa_enabled", "severity": "info", "message": "MFA enabled for user"},
        {"event": "mfa_disabled", "severity": "warn", "message": "MFA disabled for user"},
        {"event": "session_expired", "severity": "info", "message": "Session expired"},
        {"event": "token_revoked", "severity": "info", "message": "Access token revoked"},
    ],
    "authorization": [
        {"event": "access_denied", "severity": "warn", "message": "Access denied to resource"},
        {"event": "privilege_escalation", "severity": "error", "message": "Privilege escalation attempt detected"},
        {"event": "role_changed", "severity": "info", "message": "User role changed"},
        {"event": "permission_granted", "severity": "info", "message": "Permission granted to user"},
        {"event": "permission_revoked", "severity": "info", "message": "Permission revoked from user"},
    ],
    "threat_detection": [
        {"event": "sql_injection", "severity": "error", "message": "SQL injection attempt detected"},
        {"event": "xss_attempt", "severity": "error", "message": "XSS attempt detected"},
        {"event": "path_traversal", "severity": "error", "message": "Path traversal attempt detected"},
        {"event": "brute_force", "severity": "error", "message": "Brute force attack detected"},
        {"event": "credential_stuffing", "severity": "error", "message": "Credential stuffing attack detected"},
        {"event": "suspicious_ip", "severity": "warn", "message": "Request from suspicious IP address"},
        {"event": "anomalous_behavior", "severity": "warn", "message": "Anomalous user behavior detected"},
        {"event": "impossible_travel", "severity": "warn", "message": "Impossible travel detected"},
    ],
    "data_access": [
        {"event": "sensitive_data_access", "severity": "info", "message": "Sensitive data accessed"},
        {"event": "bulk_data_export", "severity": "warn", "message": "Bulk data export performed"},
        {"event": "pii_access", "severity": "info", "message": "PII data accessed"},
        {"event": "data_deletion", "severity": "warn", "message": "Data deletion performed"},
    ],
}

# =============================================================================
# HELPER CLASSES AND FUNCTIONS
# =============================================================================

@dataclass
class LogContext:
    """Shared context for generating correlated logs."""
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:32])
    span_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    parent_span_id: Optional[str] = None
    user: Optional[dict] = None
    session_id: Optional[str] = None
    request_id: str = field(default_factory=lambda: f"req_{uuid.uuid4().hex[:12]}")
    client_ip: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    environment: str = "production"
    region: str = "us-east-1"


def generate_trace_id() -> str:
    return uuid.uuid4().hex[:32]


def generate_span_id() -> str:
    return uuid.uuid4().hex[:16]


def generate_request_id() -> str:
    return f"req_{uuid.uuid4().hex[:12]}"


def generate_transaction_id() -> str:
    return f"txn_{uuid.uuid4().hex[:16]}"


def generate_order_id() -> str:
    return f"ord_{uuid.uuid4().hex[:12]}"


def get_random_ip(ip_type: str = "mixed") -> tuple[str, str]:
    """Get a random IP address and its location."""
    if ip_type == "internal":
        ip, loc = random.choice(IP_POOLS["internal"])
    elif ip_type == "suspicious":
        ip, loc = random.choice(IP_POOLS["suspicious"])
    elif ip_type == "residential":
        ip, loc = random.choice(IP_POOLS["residential"])
    else:
        pool = random.choices(
            ["internal", "residential", "office", "suspicious"],
            weights=[30, 50, 15, 5]
        )[0]
        ip, loc = random.choice(IP_POOLS[pool])
    return ip, loc


def get_random_user(user_type: str = "mixed") -> dict:
    """Get a random user."""
    if user_type == "admin":
        return random.choice(USERS["admins"])
    elif user_type == "developer":
        return random.choice(USERS["developers"])
    elif user_type == "service":
        return random.choice(USERS["service_accounts"])
    elif user_type == "customer":
        return random.choice(USERS["customers"])
    elif user_type == "suspicious":
        return random.choice(USERS["suspicious"])
    else:
        pool = random.choices(
            ["admins", "developers", "service_accounts", "customers"],
            weights=[5, 10, 15, 70]
        )[0]
        return random.choice(USERS[pool])


def get_random_user_agent(agent_type: str = "mixed") -> str:
    """Get a random user agent string."""
    if agent_type in USER_AGENTS:
        return random.choice(USER_AGENTS[agent_type])
    else:
        pool = random.choices(
            ["browsers", "mobile", "api_clients", "bots"],
            weights=[50, 20, 25, 5]
        )[0]
        return random.choice(USER_AGENTS[pool])


def generate_stack_trace(language: str, service: str) -> str:
    """Generate a realistic stack trace."""
    templates = ERROR_TEMPLATES.get(language, ERROR_TEMPLATES["python"])
    exception, message = random.choice(templates["exceptions"])
    
    template = templates["stack_template"]
    
    replacements = {
        "service": service.replace("-", "_"),
        "exception": exception,
        "message": message,
        "line1": random.randint(10, 500),
        "line2": random.randint(10, 300),
        "line3": random.randint(10, 200),
        "col1": random.randint(1, 50),
        "col2": random.randint(1, 50),
        "col3": random.randint(1, 50),
        "func1": random.choice(["handle_request", "process", "execute", "run"]),
        "func2": random.choice(["validate", "transform", "parse", "fetch"]),
        "func3": random.choice(["serialize", "convert", "format", "encode"]),
        "operation1": random.choice(["self.process(data)", "handler.execute()", "db.query(sql)"]),
        "operation2": random.choice(["json.loads(response)", "model.validate()", "cache.get(key)"]),
        "operation3": random.choice(["result.decode()", "data['value']", "obj.attribute"]),
        "handler": random.choice(["user", "order", "payment", "product"]),
        "class1": random.choice(["UserService", "OrderHandler", "PaymentProcessor"]),
        "class2": random.choice(["Repository", "Validator", "Mapper"]),
        "class3": random.choice(["Utils", "Helper", "Converter"]),
        "method1": random.choice(["process", "handle", "execute"]),
        "method2": random.choice(["validate", "transform", "map"]),
        "method3": random.choice(["convert", "serialize", "format"]),
        "package": random.choice(["handlers", "services", "utils"]),
        "file": random.choice(["handler", "service", "processor"]),
        "file1": random.choice(["handler", "controller", "service"]),
        "file2": random.choice(["validator", "repository", "mapper"]),
        "file3": random.choice(["utils", "helpers", "common"]),
        "offset": f"{random.randint(100, 999):x}",
    }
    
    for key, value in replacements.items():
        template = template.replace("{" + key + "}", str(value))
    
    return template


def calculate_latency(service: str, is_error: bool = False, is_slow: bool = False) -> int:
    """Calculate realistic latency in nanoseconds."""
    base_latencies = {
        "api-gateway": (5, 50),
        "auth-service": (10, 100),
        "user-service": (20, 150),
        "payment-service": (100, 500),
        "order-service": (50, 300),
        "search-service": (30, 200),
        "recommendation-service": (100, 400),
        "ml-inference": (200, 1000),
    }
    
    min_ms, max_ms = base_latencies.get(service, (10, 200))
    
    if is_error:
        latency_ms = random.randint(max_ms, max_ms * 3)
    elif is_slow:
        latency_ms = random.randint(max_ms * 2, max_ms * 10)
    else:
        latency_ms = random.randint(min_ms, max_ms)
    
    return latency_ms * 1_000_000  # Convert to nanoseconds


# =============================================================================
# LOG GENERATORS
# =============================================================================

def generate_http_access_logs(count: int, ctx: Optional[LogContext] = None) -> list:
    """Generate HTTP access logs (nginx/ALB style)."""
    logs = []
    
    for _ in range(count):
        endpoint = random.choice(ALL_ENDPOINTS)
        ip, location = get_random_ip()
        user_agent = get_random_user_agent()
        
        # Weight status codes realistically
        status = random.choices(
            [200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 429, 500, 502, 503, 504],
            weights=[60, 5, 3, 2, 2, 5, 5, 3, 2, 8, 2, 1, 1, 0.5, 0.5]
        )[0]
        
        is_error = status >= 400
        latency_ns = calculate_latency("api-gateway", is_error)
        
        # Determine service based on endpoint path
        if "/auth" in endpoint["path"]:
            service = "auth-service"
        elif "/users" in endpoint["path"]:
            service = "user-service"
        elif "/orders" in endpoint["path"]:
            service = "order-service"
        elif "/payments" in endpoint["path"]:
            service = "payment-service"
        elif "/products" in endpoint["path"] or "/search" in endpoint["path"]:
            service = "search-service"
        elif "/cart" in endpoint["path"]:
            service = "cart-service"
        elif "/admin" in endpoint["path"]:
            service = "api-gateway"
        else:
            service = "api-gateway"
        
        # Replace path parameters
        path = endpoint["path"]
        if "{id}" in path:
            path = path.replace("{id}", str(random.randint(1000, 99999)))
        
        request_id = generate_request_id()
        trace_id = generate_trace_id()
        
        # Calculate response size
        if status == 204:
            response_size = 0
        elif status >= 400:
            response_size = random.randint(100, 500)
        else:
            response_size = random.randint(200, 50000)
        
        log_status = "error" if status >= 500 else ("warn" if status >= 400 else "info")
        
        logs.append({
            "ddsource": "nginx",
            "ddtags": f"env:production,service:{service},region:us-east-1",
            "hostname": random.choice(HOSTS["web"]),
            "service": service,
            "status": log_status,
            "message": f'{ip} - "{endpoint["method"]} {path} HTTP/1.1" {status} {response_size}',
            "http": {
                "method": endpoint["method"],
                "url": path,
                "url_details": {
                    "path": path,
                    "scheme": "https",
                    "host": "api.company.com",
                },
                "status_code": status,
                "useragent": user_agent,
                "request_id": request_id,
                "response_size": response_size,
            },
            "network": {
                "client": {
                    "ip": ip,
                    "geoip": {
                        "country_name": location,
                    },
                },
            },
            "duration": latency_ns,
            "trace_id": trace_id,
        })
    
    return logs


def generate_application_logs(count: int) -> list:
    """Generate application-level logs from various services."""
    logs = []
    
    log_levels = ["DEBUG", "INFO", "WARN", "ERROR"]
    level_weights = [5, 70, 15, 10]
    
    info_messages = [
        "Request processed successfully",
        "Cache hit for key: {key}",
        "Database query executed in {ms}ms",
        "User {user_id} authenticated successfully",
        "Order {order_id} created",
        "Payment {payment_id} processed",
        "Email sent to {email}",
        "Webhook delivered successfully",
        "Configuration reloaded",
        "Health check passed",
        "Background job completed",
        "Session created for user {user_id}",
        "Rate limit check passed",
        "Feature flag {flag} evaluated to {value}",
        "Metrics exported successfully",
    ]
    
    warn_messages = [
        "Slow database query: {ms}ms",
        "Cache miss for key: {key}",
        "Retry attempt {n} for operation",
        "Connection pool running low: {available}/{total}",
        "Rate limit approaching for user {user_id}",
        "Deprecated API endpoint called: {endpoint}",
        "Memory usage high: {percent}%",
        "Request timeout warning: {ms}ms",
        "Circuit breaker half-open",
        "Stale cache data served",
        "Background job delayed",
        "External service latency elevated",
    ]
    
    error_messages = [
        "Failed to process request: {error}",
        "Database connection failed",
        "Cache connection refused",
        "Payment processing failed: {reason}",
        "External API returned error: {status}",
        "Message queue publish failed",
        "File upload failed: {reason}",
        "Authentication failed for user {user_id}",
        "Rate limit exceeded for IP {ip}",
        "Circuit breaker open",
        "Health check failed",
        "Background job failed: {error}",
        "Webhook delivery failed after {n} retries",
        "Data validation failed: {field}",
    ]
    
    for _ in range(count):
        level = random.choices(log_levels, weights=level_weights)[0]
        service_name = random.choice(list(FLAT_SERVICES.keys()))
        service_info = FLAT_SERVICES[service_name]
        
        if level == "DEBUG":
            message = f"Debug: {random.choice(['entering function', 'processing item', 'checking condition', 'iterating over'])}"
        elif level == "INFO":
            message = random.choice(info_messages)
        elif level == "WARN":
            message = random.choice(warn_messages)
        else:
            message = random.choice(error_messages)
        
        # Fill in placeholders
        replacements = {
            "{key}": f"user:{random.randint(1000, 9999)}",
            "{ms}": str(random.randint(50, 5000)),
            "{user_id}": f"u_{random.randint(1000, 9999)}",
            "{order_id}": generate_order_id(),
            "{payment_id}": f"pay_{uuid.uuid4().hex[:12]}",
            "{email}": f"user{random.randint(1, 100)}@example.com",
            "{flag}": random.choice(["new_checkout", "dark_mode", "beta_features"]),
            "{value}": random.choice(["true", "false"]),
            "{n}": str(random.randint(1, 5)),
            "{available}": str(random.randint(1, 10)),
            "{total}": str(random.randint(50, 100)),
            "{percent}": str(random.randint(80, 99)),
            "{endpoint}": random.choice(["/api/v1/legacy", "/api/v1/old-auth"]),
            "{error}": random.choice(["timeout", "connection refused", "invalid data"]),
            "{reason}": random.choice(["card declined", "insufficient funds", "network error"]),
            "{status}": str(random.choice([400, 401, 403, 500, 502, 503])),
            "{ip}": get_random_ip()[0],
            "{field}": random.choice(["email", "phone", "amount", "address"]),
        }
        
        for key, value in replacements.items():
            message = message.replace(key, value)
        
        trace_id = generate_trace_id()
        span_id = generate_span_id()
        
        log_entry = {
            "ddsource": service_info["language"],
            "ddtags": f"env:production,service:{service_name},version:1.2.3",
            "hostname": random.choice(ALL_HOSTS),
            "service": service_name,
            "status": level.lower(),
            "message": f"[{level}] {message}",
            "logger": {
                "name": f"{service_name}.{random.choice(['handler', 'service', 'repository'])}",
                "thread_name": f"worker-{random.randint(1, 8)}",
            },
            "trace_id": trace_id,
            "span_id": span_id,
        }
        
        if level == "ERROR":
            log_entry["error"] = {
                "message": message,
                "kind": random.choice(["RuntimeError", "ValueError", "ConnectionError"]),
                "stack": generate_stack_trace(service_info["language"], service_name),
            }
        
        logs.append(log_entry)
    
    return logs


def generate_authentication_logs(count: int) -> list:
    """Generate authentication and authorization logs."""
    logs = []
    
    for _ in range(count):
        # Select event type with realistic distribution
        event_category = random.choices(
            list(SECURITY_EVENTS.keys()),
            weights=[50, 20, 15, 15]
        )[0]
        
        event = random.choice(SECURITY_EVENTS[event_category])
        
        # Determine if this should be suspicious
        is_suspicious = random.random() < 0.1
        
        if is_suspicious:
            ip, location = get_random_ip("suspicious")
            user = get_random_user("suspicious") if random.random() < 0.3 else get_random_user()
        else:
            ip, location = get_random_ip()
            user = get_random_user()
        
        trace_id = generate_trace_id()
        session_id = f"sess_{uuid.uuid4().hex[:16]}"
        
        log_entry = {
            "ddsource": "security",
            "ddtags": f"env:production,service:auth-service,event_category:{event_category}",
            "hostname": random.choice(HOSTS["api"]),
            "service": "auth-service",
            "status": event["severity"],
            "message": f"{event['message']} - {user['email']}",
            "evt": {
                "name": event["event"],
                "category": event_category,
                "outcome": "failure" if event["severity"] in ["warn", "error"] else "success",
            },
            "usr": {
                "id": user["id"],
                "email": user["email"],
                "name": user["name"],
            },
            "network": {
                "client": {
                    "ip": ip,
                    "geoip": {
                        "country_name": location,
                        "city_name": random.choice(["New York", "London", "Tokyo", "Moscow", "Beijing", "Mumbai"]),
                    },
                },
            },
            "session_id": session_id,
            "trace_id": trace_id,
        }
        
        # Add authentication-specific fields
        if event_category == "authentication":
            log_entry["auth"] = {
                "method": random.choice(["password", "oauth", "sso", "api_key", "mfa"]),
                "provider": random.choice(["internal", "google", "github", "okta"]) if "oauth" in event["event"] or "sso" in event["event"] else "internal",
            }
        
        # Add threat detection details
        if event_category == "threat_detection":
            log_entry["threat"] = {
                "tactic": random.choice(["initial_access", "credential_access", "persistence"]),
                "technique": event["event"],
                "confidence": random.choice(["low", "medium", "high"]),
            }
            if is_suspicious:
                log_entry["threat"]["indicators"] = [
                    f"IP in threat intelligence feed",
                    f"Unusual access pattern",
                    f"Geographic anomaly",
                ]
        
        logs.append(log_entry)
    
    return logs


def generate_cloudtrail_logs(count: int) -> list:
    """Generate AWS CloudTrail-style audit logs."""
    logs = []
    
    cloudtrail_events = [
        # IAM Events
        {"name": "ConsoleLogin", "service": "signin.amazonaws.com", "category": "authentication"},
        {"name": "CreateUser", "service": "iam.amazonaws.com", "category": "iam"},
        {"name": "DeleteUser", "service": "iam.amazonaws.com", "category": "iam"},
        {"name": "AttachUserPolicy", "service": "iam.amazonaws.com", "category": "iam"},
        {"name": "CreateAccessKey", "service": "iam.amazonaws.com", "category": "iam"},
        {"name": "DeleteAccessKey", "service": "iam.amazonaws.com", "category": "iam"},
        {"name": "AssumeRole", "service": "sts.amazonaws.com", "category": "iam"},
        
        # S3 Events
        {"name": "CreateBucket", "service": "s3.amazonaws.com", "category": "s3"},
        {"name": "DeleteBucket", "service": "s3.amazonaws.com", "category": "s3"},
        {"name": "PutBucketPolicy", "service": "s3.amazonaws.com", "category": "s3"},
        {"name": "GetObject", "service": "s3.amazonaws.com", "category": "s3"},
        {"name": "PutObject", "service": "s3.amazonaws.com", "category": "s3"},
        {"name": "DeleteObject", "service": "s3.amazonaws.com", "category": "s3"},
        
        # EC2 Events
        {"name": "RunInstances", "service": "ec2.amazonaws.com", "category": "ec2"},
        {"name": "TerminateInstances", "service": "ec2.amazonaws.com", "category": "ec2"},
        {"name": "StopInstances", "service": "ec2.amazonaws.com", "category": "ec2"},
        {"name": "CreateSecurityGroup", "service": "ec2.amazonaws.com", "category": "ec2"},
        {"name": "AuthorizeSecurityGroupIngress", "service": "ec2.amazonaws.com", "category": "ec2"},
        {"name": "ModifyInstanceAttribute", "service": "ec2.amazonaws.com", "category": "ec2"},
        
        # RDS Events
        {"name": "CreateDBInstance", "service": "rds.amazonaws.com", "category": "rds"},
        {"name": "DeleteDBInstance", "service": "rds.amazonaws.com", "category": "rds"},
        {"name": "ModifyDBInstance", "service": "rds.amazonaws.com", "category": "rds"},
        {"name": "CreateDBSnapshot", "service": "rds.amazonaws.com", "category": "rds"},
        
        # Lambda Events
        {"name": "CreateFunction", "service": "lambda.amazonaws.com", "category": "lambda"},
        {"name": "UpdateFunctionCode", "service": "lambda.amazonaws.com", "category": "lambda"},
        {"name": "DeleteFunction", "service": "lambda.amazonaws.com", "category": "lambda"},
        {"name": "Invoke", "service": "lambda.amazonaws.com", "category": "lambda"},
        
        # Secrets Manager
        {"name": "GetSecretValue", "service": "secretsmanager.amazonaws.com", "category": "secrets"},
        {"name": "CreateSecret", "service": "secretsmanager.amazonaws.com", "category": "secrets"},
        {"name": "DeleteSecret", "service": "secretsmanager.amazonaws.com", "category": "secrets"},
        
        # KMS Events
        {"name": "Decrypt", "service": "kms.amazonaws.com", "category": "kms"},
        {"name": "Encrypt", "service": "kms.amazonaws.com", "category": "kms"},
        {"name": "CreateKey", "service": "kms.amazonaws.com", "category": "kms"},
    ]
    
    for _ in range(count):
        event = random.choice(cloudtrail_events)
        
        is_error = random.random() < 0.08
        is_suspicious = random.random() < 0.05
        
        if is_suspicious:
            ip, location = get_random_ip("suspicious")
            user = random.choice(USERS["suspicious"] + USERS["admins"])
        else:
            ip, location = get_random_ip("residential" if random.random() < 0.3 else "internal")
            user = random.choice(USERS["admins"] + USERS["developers"] + USERS["service_accounts"])
        
        region = random.choice(REGIONS)
        
        user_identity_type = random.choice(["IAMUser", "AssumedRole", "Root", "AWSService"])
        
        log_entry = {
            "ddsource": "cloudtrail",
            "ddtags": f"env:production,service:aws,cloud:aws,region:{region}",
            "hostname": "cloudtrail",
            "service": "aws",
            "source": "cloudtrail",
            "status": "error" if is_error else "info",
            "message": f"AWS {event['name']} by {user['email']} from {location}",
            "evt": {
                "name": event["name"],
                "outcome": "failure" if is_error else "success",
                "category": "cloud",
            },
            "cloud": {
                "provider": "aws",
                "region": region,
                "account_id": "123456789012",
            },
            "userIdentity": {
                "type": user_identity_type,
                "arn": f"arn:aws:iam::123456789012:user/{user['id']}",
                "accountId": "123456789012",
                "userName": user["id"],
                "principalId": f"AIDA{uuid.uuid4().hex[:17].upper()}",
            },
            "eventSource": event["service"],
            "eventName": event["name"],
            "eventCategory": event["category"],
            "awsRegion": region,
            "sourceIPAddress": ip,
            "userAgent": random.choice([
                "console.amazonaws.com",
                "aws-cli/2.13.0 Python/3.11.4",
                "Boto3/1.28.0 Python/3.10.0",
                "terraform/1.5.0",
            ]),
            "network": {
                "client": {
                    "ip": ip,
                    "geoip": {
                        "country_name": location,
                    },
                },
            },
        }
        
        # Add error details
        if is_error:
            error_codes = {
                "iam": ["AccessDenied", "MalformedPolicyDocument", "EntityAlreadyExists"],
                "s3": ["AccessDenied", "NoSuchBucket", "BucketAlreadyExists"],
                "ec2": ["UnauthorizedOperation", "InvalidParameterValue", "InsufficientInstanceCapacity"],
                "rds": ["DBInstanceAlreadyExists", "InvalidDBInstanceState", "StorageQuotaExceeded"],
                "lambda": ["ResourceNotFoundException", "InvalidParameterValueException"],
                "secrets": ["ResourceNotFoundException", "AccessDeniedException"],
                "kms": ["AccessDeniedException", "NotFoundException"],
            }
            category_errors = error_codes.get(event["category"], ["UnauthorizedOperation"])
            log_entry["errorCode"] = random.choice(category_errors)
            log_entry["errorMessage"] = "User is not authorized to perform this operation"
        
        # Add resource-specific details
        if event["category"] == "s3":
            log_entry["requestParameters"] = {
                "bucketName": random.choice(AWS_RESOURCES["s3_buckets"]),
            }
            if "Object" in event["name"]:
                log_entry["requestParameters"]["key"] = f"data/{random.choice(['uploads', 'exports', 'logs'])}/{uuid.uuid4().hex[:8]}.json"
        
        elif event["category"] == "ec2":
            log_entry["requestParameters"] = {
                "instancesSet": {"items": [{"instanceId": f"i-{uuid.uuid4().hex[:17]}"}]},
                "instanceType": random.choice(AWS_RESOURCES["ec2_instance_types"]),
            }
        
        elif event["category"] == "lambda":
            log_entry["requestParameters"] = {
                "functionName": random.choice(AWS_RESOURCES["lambda_functions"]),
            }
        
        logs.append(log_entry)
    
    return logs


def generate_database_logs(count: int) -> list:
    """Generate database operation logs."""
    logs = []
    
    for _ in range(count):
        db_type = random.choice(list(DATABASES.keys()))
        db_config = DATABASES[db_type]
        
        is_slow = random.random() < 0.15
        is_error = random.random() < 0.05
        
        if db_type in ["postgresql", "mysql"]:
            operations = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE INDEX", "ANALYZE", "VACUUM"]
            tables = ["users", "orders", "products", "transactions", "sessions", "audit_log", "inventory"]
            
            operation = random.choice(operations)
            table = random.choice(tables)
            
            if is_slow:
                duration_ms = random.randint(1000, 30000)
                message = f"Slow query: {operation} on {table} took {duration_ms}ms"
                status = "warn"
            elif is_error:
                error = random.choice([
                    "deadlock detected",
                    "lock wait timeout exceeded",
                    "duplicate key violation",
                    "foreign key constraint failed",
                    "connection pool exhausted",
                    "too many connections",
                ])
                duration_ms = random.randint(100, 500)
                message = f"Query error: {error}"
                status = "error"
            else:
                duration_ms = random.randint(1, 100)
                message = f"Query executed: {operation} on {table}"
                status = "info"
            
            log_entry = {
                "ddsource": db_type,
                "ddtags": f"env:production,service:{db_type},database:{random.choice(db_config['databases'])}",
                "hostname": random.choice(db_config["hosts"]),
                "service": db_type,
                "status": status,
                "message": message,
                "db": {
                    "type": db_type,
                    "name": random.choice(db_config["databases"]),
                    "operation": operation,
                    "table": table,
                    "rows_affected": random.randint(0, 10000) if operation != "SELECT" else None,
                    "rows_returned": random.randint(0, 1000) if operation == "SELECT" else None,
                },
                "duration": duration_ms * 1_000_000,
            }
        
        elif db_type == "redis":
            operations = ["GET", "SET", "DEL", "HGET", "HSET", "LPUSH", "LPOP", "EXPIRE", "TTL"]
            operation = random.choice(operations)
            
            if is_error:
                message = random.choice([
                    "READONLY You can't write against a read only replica",
                    "OOM command not allowed when used memory > maxmemory",
                    "CLUSTERDOWN The cluster is down",
                ])
                status = "error"
            else:
                key = f"{random.choice(['user', 'session', 'cache', 'rate_limit'])}:{random.randint(1000, 9999)}"
                duration_us = random.randint(50, 500)
                message = f"{operation} {key} ({duration_us}μs)"
                status = "info"
            
            log_entry = {
                "ddsource": "redis",
                "ddtags": "env:production,service:redis",
                "hostname": random.choice(db_config["hosts"]),
                "service": "redis",
                "status": status,
                "message": message,
                "db": {
                    "type": "redis",
                    "operation": operation,
                },
            }
        
        elif db_type == "elasticsearch":
            operations = ["search", "index", "delete", "bulk", "scroll", "reindex"]
            operation = random.choice(operations)
            index = random.choice(db_config["indices"])
            
            if is_slow:
                took_ms = random.randint(5000, 30000)
                message = f"Slow {operation} on {index}: {took_ms}ms"
                status = "warn"
            elif is_error:
                error = random.choice([
                    "index_not_found_exception",
                    "search_phase_execution_exception",
                    "cluster_block_exception",
                ])
                message = f"Error: {error} on {index}"
                status = "error"
            else:
                took_ms = random.randint(5, 500)
                message = f"{operation.capitalize()} on {index}: {took_ms}ms"
                status = "info"
            
            log_entry = {
                "ddsource": "elasticsearch",
                "ddtags": f"env:production,service:elasticsearch,index:{index}",
                "hostname": random.choice(db_config["hosts"]),
                "service": "elasticsearch",
                "status": status,
                "message": message,
                "elasticsearch": {
                    "index": index,
                    "operation": operation,
                    "took_ms": took_ms if "took_ms" in dir() else None,
                    "hits": random.randint(0, 10000) if operation == "search" else None,
                },
            }
        
        else:  # mongodb, cassandra
            operations = ["find", "insert", "update", "delete", "aggregate"]
            operation = random.choice(operations)
            
            duration_ms = random.randint(1000, 10000) if is_slow else random.randint(1, 100)
            message = f"{operation} on {random.choice(db_config.get('databases', db_config.get('keyspaces', ['data'])))} ({duration_ms}ms)"
            status = "warn" if is_slow else "info"
            
            log_entry = {
                "ddsource": db_type,
                "ddtags": f"env:production,service:{db_type}",
                "hostname": random.choice(db_config["hosts"]),
                "service": db_type,
                "status": status,
                "message": message,
                "db": {
                    "type": db_type,
                    "operation": operation,
                },
                "duration": duration_ms * 1_000_000,
            }
        
        logs.append(log_entry)
    
    return logs


def generate_kubernetes_logs(count: int) -> list:
    """Generate Kubernetes cluster event logs."""
    logs = []
    
    k8s_events = [
        # Pod events
        {"type": "Normal", "reason": "Scheduled", "message": "Successfully assigned {namespace}/{pod} to {node}", "status": "info"},
        {"type": "Normal", "reason": "Pulling", "message": "Pulling image \"{image}\"", "status": "info"},
        {"type": "Normal", "reason": "Pulled", "message": "Successfully pulled image \"{image}\"", "status": "info"},
        {"type": "Normal", "reason": "Created", "message": "Created container {container}", "status": "info"},
        {"type": "Normal", "reason": "Started", "message": "Started container {container}", "status": "info"},
        {"type": "Normal", "reason": "Killing", "message": "Stopping container {container}", "status": "info"},
        {"type": "Warning", "reason": "BackOff", "message": "Back-off restarting failed container {container}", "status": "warn"},
        {"type": "Warning", "reason": "Failed", "message": "Error: ImagePullBackOff", "status": "error"},
        {"type": "Warning", "reason": "FailedScheduling", "message": "0/{nodes} nodes are available: {nodes} Insufficient cpu", "status": "error"},
        {"type": "Warning", "reason": "Unhealthy", "message": "Liveness probe failed: HTTP probe failed with statuscode: 503", "status": "error"},
        {"type": "Warning", "reason": "OOMKilled", "message": "Container {container} was OOM killed", "status": "error"},
        {"type": "Warning", "reason": "CrashLoopBackOff", "message": "Back-off 5m0s restarting failed container", "status": "error"},
        
        # Deployment events
        {"type": "Normal", "reason": "ScalingReplicaSet", "message": "Scaled up replica set {deployment} to {replicas}", "status": "info"},
        {"type": "Normal", "reason": "ScalingReplicaSet", "message": "Scaled down replica set {deployment} to {replicas}", "status": "info"},
        
        # Node events
        {"type": "Normal", "reason": "NodeReady", "message": "Node {node} status is now: NodeReady", "status": "info"},
        {"type": "Warning", "reason": "NodeNotReady", "message": "Node {node} status is now: NodeNotReady", "status": "warn"},
        {"type": "Warning", "reason": "NodeHasDiskPressure", "message": "Node {node} has disk pressure", "status": "warn"},
        {"type": "Warning", "reason": "NodeHasMemoryPressure", "message": "Node {node} has memory pressure", "status": "warn"},
        
        # HPA events
        {"type": "Normal", "reason": "SuccessfulRescale", "message": "New size: {replicas}; reason: CPU utilization above target", "status": "info"},
        
        # PVC events
        {"type": "Normal", "reason": "ProvisioningSucceeded", "message": "Successfully provisioned volume pvc-{pvc_id}", "status": "info"},
        {"type": "Warning", "reason": "ProvisioningFailed", "message": "Failed to provision volume: {error}", "status": "error"},
    ]
    
    services = list(FLAT_SERVICES.keys())
    
    for _ in range(count):
        event = random.choice(k8s_events)
        namespace = random.choice(K8S_NAMESPACES)
        cluster = random.choice(K8S_CLUSTERS)
        
        # Generate pod and deployment names
        service = random.choice(services)
        deployment = f"{service}-deployment"
        pod = f"{service}-{uuid.uuid4().hex[:8]}"
        container = service.replace("-service", "")
        node = f"gke-{cluster}-{random.choice(K8S_NODE_POOLS)}-{uuid.uuid4().hex[:8]}"
        
        message = event["message"]
        replacements = {
            "{namespace}": namespace,
            "{pod}": pod,
            "{node}": node,
            "{image}": f"gcr.io/company/{service}:v1.{random.randint(0, 99)}.{random.randint(0, 999)}",
            "{container}": container,
            "{deployment}": deployment,
            "{replicas}": str(random.randint(1, 10)),
            "{nodes}": str(random.randint(3, 10)),
            "{pvc_id}": uuid.uuid4().hex[:8],
            "{error}": random.choice(["no storage class found", "quota exceeded", "invalid access mode"]),
        }
        
        for key, value in replacements.items():
            message = message.replace(key, value)
        
        logs.append({
            "ddsource": "kubernetes",
            "ddtags": f"env:production,kube_cluster:{cluster},kube_namespace:{namespace},service:{service}",
            "hostname": node,
            "service": "kubernetes",
            "status": event["status"],
            "message": f"[{namespace}/{pod}] {event['type']}: {event['reason']} - {message}",
            "kubernetes": {
                "cluster_name": cluster,
                "namespace_name": namespace,
                "pod_name": pod,
                "container_name": container,
                "node_name": node,
                "event_type": event["type"],
                "event_reason": event["reason"],
            },
        })
    
    return logs


def generate_kafka_logs(count: int) -> list:
    """Generate Kafka message broker logs."""
    logs = []
    
    kafka_config = MESSAGE_QUEUES["kafka"]
    
    for _ in range(count):
        topic = random.choice(kafka_config["topics"])
        broker = random.choice(kafka_config["brokers"])
        partition = random.randint(0, 5)
        
        is_error = random.random() < 0.05
        is_lag = random.random() < 0.1
        
        if is_error:
            errors = [
                "Failed to send message: Connection refused",
                "Consumer group rebalance in progress",
                "Message size exceeds max.message.bytes",
                "Producer fenced: older producer epoch",
                "Broker not available",
            ]
            message = random.choice(errors)
            status = "error"
        elif is_lag:
            lag = random.randint(10000, 100000)
            message = f"Consumer lag detected on topic {topic}: {lag} messages behind"
            status = "warn"
        else:
            event_types = [
                ("Message produced", "info"),
                ("Message consumed", "info"),
                ("Batch processed", "info"),
                ("Consumer joined group", "info"),
                ("Offset committed", "info"),
            ]
            msg, status = random.choice(event_types)
            message = f"{msg}: topic={topic} partition={partition}"
        
        logs.append({
            "ddsource": "kafka",
            "ddtags": f"env:production,service:kafka,topic:{topic}",
            "hostname": broker,
            "service": "kafka",
            "status": status,
            "message": message,
            "kafka": {
                "topic": topic,
                "partition": partition,
                "broker": broker,
                "consumer_group": f"{topic.split('.')[0]}-consumers",
            },
        })
    
    return logs


def generate_lambda_logs(count: int) -> list:
    """Generate AWS Lambda function logs."""
    logs = []
    
    for _ in range(count):
        function = random.choice(AWS_RESOURCES["lambda_functions"])
        request_id = str(uuid.uuid4())
        
        is_error = random.random() < 0.08
        is_timeout = random.random() < 0.03
        is_cold_start = random.random() < 0.15
        
        duration_ms = random.randint(100, 5000)
        billed_duration = ((duration_ms // 100) + 1) * 100
        memory_used = random.randint(64, 512)
        memory_allocated = random.choice([128, 256, 512, 1024])
        
        if is_timeout:
            message = f"Task timed out after {random.choice([3, 10, 30, 60])}.00 seconds"
            status = "error"
        elif is_error:
            errors = [
                "Runtime.UnhandledPromiseRejection",
                "Runtime.ImportModuleError",
                "Runtime.HandlerNotFound",
                "Lambda.ServiceException",
            ]
            message = f"RequestId: {request_id} Error: {random.choice(errors)}"
            status = "error"
        else:
            if is_cold_start:
                init_duration = random.randint(200, 2000)
                message = f"REPORT RequestId: {request_id} Duration: {duration_ms} ms Billed Duration: {billed_duration} ms Memory Size: {memory_allocated} MB Max Memory Used: {memory_used} MB Init Duration: {init_duration} ms"
            else:
                message = f"REPORT RequestId: {request_id} Duration: {duration_ms} ms Billed Duration: {billed_duration} ms Memory Size: {memory_allocated} MB Max Memory Used: {memory_used} MB"
            status = "info"
        
        logs.append({
            "ddsource": "lambda",
            "ddtags": f"env:production,service:lambda,function:{function},region:{random.choice(REGIONS)}",
            "hostname": f"lambda-{function}",
            "service": f"lambda-{function}",
            "status": status,
            "message": message,
            "lambda": {
                "function_name": function,
                "request_id": request_id,
                "memory_allocated": memory_allocated,
                "memory_used": memory_used,
                "duration_ms": duration_ms,
                "billed_duration_ms": billed_duration,
                "cold_start": is_cold_start,
            },
        })
    
    return logs


def generate_cicd_logs(count: int) -> list:
    """Generate CI/CD pipeline logs."""
    logs = []
    
    pipelines = ["build", "test", "deploy", "release"]
    stages = {
        "build": ["checkout", "install", "compile", "lint", "build-image", "push-image"],
        "test": ["unit-tests", "integration-tests", "e2e-tests", "security-scan", "coverage"],
        "deploy": ["validate", "plan", "apply", "smoke-test", "health-check"],
        "release": ["tag", "changelog", "publish", "notify"],
    }
    
    services = list(FLAT_SERVICES.keys())
    
    for _ in range(count):
        pipeline = random.choice(pipelines)
        stage = random.choice(stages[pipeline])
        service = random.choice(services)
        build_number = random.randint(1000, 9999)
        
        is_error = random.random() < 0.1
        is_slow = random.random() < 0.15
        
        duration_s = random.randint(60, 600) if is_slow else random.randint(10, 120)
        
        if is_error:
            errors = {
                "build": ["Compilation failed", "Lint errors found", "Image push failed"],
                "test": ["Tests failed: 5 failures", "Coverage below threshold: 72%", "Security vulnerability found"],
                "deploy": ["Terraform apply failed", "Health check failed", "Rollback triggered"],
                "release": ["Tag already exists", "Publish failed", "Notification error"],
            }
            message = f"[{pipeline}/{stage}] {random.choice(errors[pipeline])}"
            status = "error"
        else:
            message = f"[{pipeline}/{stage}] Completed successfully in {duration_s}s"
            status = "info"
        
        commit_sha = uuid.uuid4().hex[:7]
        branch = random.choice(["main", "develop", f"feature/{random.choice(['auth', 'payments', 'ui'])}", "release/v1.2"])
        
        logs.append({
            "ddsource": "cicd",
            "ddtags": f"env:ci,service:{service},pipeline:{pipeline},stage:{stage}",
            "hostname": "github-actions",
            "service": f"cicd-{service}",
            "status": status,
            "message": message,
            "cicd": {
                "pipeline": pipeline,
                "stage": stage,
                "build_number": build_number,
                "commit": commit_sha,
                "branch": branch,
                "duration_seconds": duration_s,
                "triggered_by": random.choice(["push", "pull_request", "schedule", "manual"]),
            },
            "git": {
                "commit": commit_sha,
                "branch": branch,
                "repository": f"company/{service}",
            },
        })
    
    return logs


def generate_payment_logs(count: int) -> list:
    """Generate payment processing logs with transaction details."""
    logs = []
    
    payment_statuses = [
        ("completed", "info", 70),
        ("pending", "info", 10),
        ("failed", "error", 10),
        ("declined", "warn", 5),
        ("refunded", "info", 3),
        ("disputed", "warn", 2),
    ]
    
    payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
    card_brands = ["visa", "mastercard", "amex", "discover"]
    decline_reasons = [
        "insufficient_funds", "card_expired", "invalid_cvv", "fraud_suspected",
        "do_not_honor", "lost_card", "stolen_card", "processing_error",
    ]
    
    for _ in range(count):
        status_info = random.choices(
            payment_statuses,
            weights=[s[2] for s in payment_statuses]
        )[0]
        
        txn_status, log_status, _ = status_info
        txn_id = generate_transaction_id()
        order_id = generate_order_id()
        user = get_random_user("customer")
        
        amount = round(random.uniform(5.00, 2000.00), 2)
        currency = random.choice(["USD", "EUR", "GBP", "CAD", "AUD"])
        payment_method = random.choice(payment_methods)
        
        message = f"Payment {txn_status}: {txn_id} - ${amount:.2f} {currency}"
        
        log_entry = {
            "ddsource": "payment",
            "ddtags": f"env:production,service:payment-service,payment_method:{payment_method}",
            "hostname": random.choice(HOSTS["api"]),
            "service": "payment-service",
            "status": log_status,
            "message": message,
            "transaction": {
                "id": txn_id,
                "order_id": order_id,
                "status": txn_status,
                "amount": amount,
                "currency": currency,
                "payment_method": payment_method,
            },
            "usr": {
                "id": user["id"],
                "email": user["email"],
            },
            "trace_id": generate_trace_id(),
        }
        
        if payment_method in ["credit_card", "debit_card"]:
            log_entry["transaction"]["card"] = {
                "brand": random.choice(card_brands),
                "last_four": f"{random.randint(1000, 9999)}",
                "exp_month": random.randint(1, 12),
                "exp_year": random.randint(2024, 2030),
            }
        
        if txn_status in ["failed", "declined"]:
            log_entry["error"] = {
                "code": random.choice(decline_reasons),
                "message": f"Payment {txn_status}: {random.choice(decline_reasons).replace('_', ' ')}",
            }
        
        logs.append(log_entry)
    
    return logs


def generate_cdn_logs(count: int) -> list:
    """Generate CDN/CloudFront access logs."""
    logs = []
    
    static_paths = [
        "/static/js/app.bundle.js",
        "/static/js/vendor.bundle.js",
        "/static/css/main.css",
        "/static/css/vendor.css",
        "/static/images/logo.png",
        "/static/images/hero.jpg",
        "/static/fonts/roboto.woff2",
        "/api/v1/config.json",
        "/favicon.ico",
        "/robots.txt",
    ]
    
    result_types = [
        ("Hit", 80),
        ("Miss", 10),
        ("RefreshHit", 5),
        ("Error", 3),
        ("LimitExceeded", 2),
    ]
    
    for _ in range(count):
        path = random.choice(static_paths)
        result_type = random.choices(
            [r[0] for r in result_types],
            weights=[r[1] for r in result_types]
        )[0]
        
        ip, location = get_random_ip("residential")
        pop = random.choice(NETWORK["cdn_pops"])
        
        if result_type == "Error":
            status_code = random.choice([403, 404, 500, 502, 503])
            status = "error"
        elif result_type == "LimitExceeded":
            status_code = 429
            status = "warn"
        else:
            status_code = 200
            status = "info"
        
        bytes_sent = random.randint(1000, 5000000) if status_code == 200 else random.randint(100, 1000)
        time_taken = random.uniform(0.001, 0.5) if result_type == "Hit" else random.uniform(0.1, 2.0)
        
        logs.append({
            "ddsource": "cloudfront",
            "ddtags": f"env:production,service:cdn,pop:{pop}",
            "hostname": f"cloudfront-{pop.lower()}",
            "service": "cdn",
            "status": status,
            "message": f'{ip} {path} {status_code} {result_type} {bytes_sent}B {time_taken:.3f}s',
            "http": {
                "method": "GET",
                "url": path,
                "status_code": status_code,
                "useragent": get_random_user_agent("browsers"),
            },
            "cdn": {
                "distribution_id": random.choice(AWS_RESOURCES["cloudfront_distributions"]),
                "pop": pop,
                "result_type": result_type,
                "bytes_sent": bytes_sent,
                "time_taken_seconds": round(time_taken, 3),
            },
            "network": {
                "client": {
                    "ip": ip,
                    "geoip": {
                        "country_name": location,
                    },
                },
            },
        })
    
    return logs


def generate_waf_logs(count: int) -> list:
    """Generate WAF (Web Application Firewall) logs."""
    logs = []
    
    rule_groups = {
        "AWS-AWSManagedRulesCommonRuleSet": [
            "SizeRestrictions_BODY", "CrossSiteScripting_BODY", "CrossSiteScripting_QUERYARGUMENTS",
        ],
        "AWS-AWSManagedRulesSQLiRuleSet": [
            "SQLi_BODY", "SQLi_QUERYARGUMENTS", "SQLi_COOKIE",
        ],
        "AWS-AWSManagedRulesKnownBadInputsRuleSet": [
            "Log4JRCE_BODY", "Log4JRCE_HEADER", "Host_localhost_HEADER",
        ],
        "Custom-RateLimitRule": [
            "RateLimit-PerIP", "RateLimit-PerUser",
        ],
        "Custom-GeoBlockRule": [
            "GeoBlock-HighRiskCountries",
        ],
    }
    
    actions = [
        ("ALLOW", "info", 70),
        ("COUNT", "info", 10),
        ("BLOCK", "warn", 15),
        ("CAPTCHA", "info", 5),
    ]
    
    for _ in range(count):
        action_info = random.choices(
            actions,
            weights=[a[2] for a in actions]
        )[0]
        
        action, log_status, _ = action_info
        
        # Suspicious IPs more likely to be blocked
        if action == "BLOCK":
            ip, location = get_random_ip("suspicious") if random.random() < 0.7 else get_random_ip()
        else:
            ip, location = get_random_ip()
        
        rule_group = random.choice(list(rule_groups.keys()))
        rule_id = random.choice(rule_groups[rule_group])
        
        endpoint = random.choice(ALL_ENDPOINTS)
        path = endpoint["path"].replace("{id}", str(random.randint(1, 9999)))
        
        logs.append({
            "ddsource": "waf",
            "ddtags": f"env:production,service:waf,action:{action.lower()}",
            "hostname": "aws-waf",
            "service": "waf",
            "status": log_status,
            "message": f"WAF {action}: {rule_id} - {ip} -> {path}",
            "waf": {
                "action": action,
                "rule_group": rule_group,
                "rule_id": rule_id,
                "web_acl": "prod-api-waf",
            },
            "http": {
                "method": endpoint["method"],
                "url": path,
                "useragent": get_random_user_agent("suspicious") if action == "BLOCK" else get_random_user_agent(),
            },
            "network": {
                "client": {
                    "ip": ip,
                    "geoip": {
                        "country_name": location,
                    },
                },
            },
        })
    
    return logs


def generate_load_balancer_logs(count: int) -> list:
    """Generate Application Load Balancer logs."""
    logs = []
    
    for _ in range(count):
        endpoint = random.choice(ALL_ENDPOINTS)
        path = endpoint["path"].replace("{id}", str(random.randint(1, 9999)))
        
        ip, location = get_random_ip()
        
        # Target selection
        target_service = random.choice(list(FLAT_SERVICES.keys()))
        target_ip = f"10.0.{random.randint(1, 10)}.{random.randint(1, 254)}"
        target_port = FLAT_SERVICES[target_service]["port"]
        
        # Status codes
        elb_status = random.choices(
            [200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503, 504],
            weights=[50, 5, 3, 2, 2, 5, 3, 2, 5, 3, 5, 5, 10]
        )[0]
        
        target_status = elb_status if elb_status < 500 else random.choice([200, 500, 502, 503])
        
        request_processing_time = random.uniform(0.001, 0.01)
        target_processing_time = random.uniform(0.01, 2.0) if elb_status != 504 else 30.0
        response_processing_time = random.uniform(0.001, 0.1)
        
        if elb_status >= 500:
            log_status = "error"
        elif elb_status >= 400:
            log_status = "warn"
        else:
            log_status = "info"
        
        request_size = random.randint(100, 10000)
        response_size = random.randint(200, 100000) if elb_status < 400 else random.randint(100, 500)
        
        logs.append({
            "ddsource": "elb",
            "ddtags": f"env:production,service:alb,target_service:{target_service}",
            "hostname": random.choice(AWS_RESOURCES["elb"]),
            "service": "alb",
            "status": log_status,
            "message": f'{ip}:{random.randint(1024, 65535)} {target_ip}:{target_port} {endpoint["method"]} {path} {elb_status} {target_status}',
            "http": {
                "method": endpoint["method"],
                "url": path,
                "status_code": elb_status,
            },
            "elb": {
                "name": random.choice(AWS_RESOURCES["elb"]),
                "target_ip": target_ip,
                "target_port": target_port,
                "target_status_code": target_status,
                "request_processing_time": round(request_processing_time, 6),
                "target_processing_time": round(target_processing_time, 6),
                "response_processing_time": round(response_processing_time, 6),
                "request_size": request_size,
                "response_size": response_size,
            },
            "network": {
                "client": {
                    "ip": ip,
                    "port": random.randint(1024, 65535),
                },
            },
            "duration": int((request_processing_time + target_processing_time + response_processing_time) * 1_000_000_000),
        })
    
    return logs


def generate_batch_job_logs(count: int) -> list:
    """Generate batch job and scheduled task logs."""
    logs = []
    
    jobs = [
        {"name": "daily-report-generator", "schedule": "0 6 * * *", "typical_duration": (300, 1800)},
        {"name": "data-cleanup", "schedule": "0 2 * * *", "typical_duration": (60, 300)},
        {"name": "index-rebuilder", "schedule": "0 3 * * 0", "typical_duration": (600, 3600)},
        {"name": "backup-database", "schedule": "0 1 * * *", "typical_duration": (300, 900)},
        {"name": "sync-inventory", "schedule": "*/15 * * * *", "typical_duration": (30, 180)},
        {"name": "send-digest-emails", "schedule": "0 8 * * *", "typical_duration": (60, 600)},
        {"name": "refresh-materialized-views", "schedule": "0 */4 * * *", "typical_duration": (120, 600)},
        {"name": "archive-old-data", "schedule": "0 4 * * 0", "typical_duration": (1800, 7200)},
        {"name": "update-search-index", "schedule": "*/30 * * * *", "typical_duration": (60, 300)},
        {"name": "process-webhooks-retry", "schedule": "*/5 * * * *", "typical_duration": (10, 60)},
    ]
    
    for _ in range(count):
        job = random.choice(jobs)
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        
        is_error = random.random() < 0.08
        is_slow = random.random() < 0.1
        
        min_dur, max_dur = job["typical_duration"]
        if is_slow:
            duration_s = random.randint(max_dur, max_dur * 3)
        else:
            duration_s = random.randint(min_dur, max_dur)
        
        if is_error:
            errors = [
                "Connection to database failed",
                "Timeout waiting for lock",
                "Out of memory",
                "External API unavailable",
                "Data validation failed",
            ]
            message = f"Job {job['name']} failed: {random.choice(errors)}"
            status = "error"
            outcome = "failure"
        elif is_slow:
            message = f"Job {job['name']} completed slowly in {duration_s}s"
            status = "warn"
            outcome = "success"
        else:
            message = f"Job {job['name']} completed successfully in {duration_s}s"
            status = "info"
            outcome = "success"
        
        items_processed = random.randint(100, 100000) if not is_error else random.randint(0, 100)
        
        logs.append({
            "ddsource": "batch",
            "ddtags": f"env:production,service:scheduler,job:{job['name']}",
            "hostname": random.choice(HOSTS["worker"]),
            "service": "scheduler",
            "status": status,
            "message": message,
            "job": {
                "id": job_id,
                "name": job["name"],
                "schedule": job["schedule"],
                "outcome": outcome,
                "duration_seconds": duration_s,
                "items_processed": items_processed,
            },
        })
    
    return logs


def generate_audit_logs(count: int) -> list:
    """Generate audit trail logs for compliance."""
    logs = []
    
    audit_events = [
        # Data access
        {"action": "data.view", "resource": "user_profile", "sensitivity": "pii"},
        {"action": "data.export", "resource": "customer_list", "sensitivity": "pii"},
        {"action": "data.download", "resource": "report", "sensitivity": "internal"},
        {"action": "data.delete", "resource": "user_account", "sensitivity": "pii"},
        
        # Configuration changes
        {"action": "config.update", "resource": "system_settings", "sensitivity": "internal"},
        {"action": "config.update", "resource": "security_policy", "sensitivity": "confidential"},
        {"action": "config.update", "resource": "api_keys", "sensitivity": "confidential"},
        
        # User management
        {"action": "user.create", "resource": "user_account", "sensitivity": "internal"},
        {"action": "user.update", "resource": "user_permissions", "sensitivity": "internal"},
        {"action": "user.delete", "resource": "user_account", "sensitivity": "internal"},
        {"action": "user.impersonate", "resource": "user_session", "sensitivity": "confidential"},
        
        # API access
        {"action": "api.access", "resource": "customer_api", "sensitivity": "internal"},
        {"action": "api.rate_limit", "resource": "api_endpoint", "sensitivity": "internal"},
        
        # Administrative
        {"action": "admin.access", "resource": "admin_panel", "sensitivity": "confidential"},
        {"action": "admin.override", "resource": "security_control", "sensitivity": "confidential"},
    ]
    
    for _ in range(count):
        event = random.choice(audit_events)
        user = random.choice(USERS["admins"] + USERS["developers"])
        ip, location = get_random_ip("internal" if random.random() < 0.7 else "residential")
        
        is_suspicious = random.random() < 0.05
        if is_suspicious:
            event = random.choice([e for e in audit_events if e["sensitivity"] == "confidential"])
            ip, location = get_random_ip("suspicious")
        
        target_id = f"res_{uuid.uuid4().hex[:12]}"
        
        logs.append({
            "ddsource": "audit",
            "ddtags": f"env:production,service:audit-service,action:{event['action']},sensitivity:{event['sensitivity']}",
            "hostname": random.choice(HOSTS["api"]),
            "service": "audit-service",
            "status": "warn" if is_suspicious else "info",
            "message": f"Audit: {event['action']} on {event['resource']} by {user['email']}",
            "audit": {
                "action": event["action"],
                "resource_type": event["resource"],
                "resource_id": target_id,
                "sensitivity": event["sensitivity"],
                "outcome": "success",
            },
            "usr": {
                "id": user["id"],
                "email": user["email"],
                "name": user["name"],
                "role": user["role"],
            },
            "network": {
                "client": {
                    "ip": ip,
                    "geoip": {
                        "country_name": location,
                    },
                },
            },
            "trace_id": generate_trace_id(),
        })
    
    return logs


def generate_network_flow_logs(count: int) -> list:
    """Generate VPC flow logs / network flow logs."""
    logs = []
    
    protocols = [
        (6, "TCP", 80),
        (6, "TCP", 443),
        (6, "TCP", 5432),
        (6, "TCP", 6379),
        (6, "TCP", 9092),
        (17, "UDP", 53),
        (17, "UDP", 123),
        (1, "ICMP", 0),
    ]
    
    for _ in range(count):
        protocol_num, protocol_name, typical_port = random.choice(protocols)
        
        # Generate source and destination
        is_inbound = random.random() < 0.5
        
        if is_inbound:
            src_ip, src_loc = get_random_ip("residential")
            dst_ip = f"10.0.{random.randint(1, 10)}.{random.randint(1, 254)}"
            src_port = random.randint(1024, 65535)
            dst_port = typical_port
        else:
            src_ip = f"10.0.{random.randint(1, 10)}.{random.randint(1, 254)}"
            dst_ip, _ = get_random_ip("residential")
            src_port = typical_port
            dst_port = random.randint(1024, 65535)
        
        # Action
        action = random.choices(["ACCEPT", "REJECT"], weights=[95, 5])[0]
        
        packets = random.randint(1, 1000)
        bytes_transferred = packets * random.randint(40, 1500)
        
        status = "info" if action == "ACCEPT" else "warn"
        
        logs.append({
            "ddsource": "vpc-flow",
            "ddtags": f"env:production,service:vpc,action:{action.lower()}",
            "hostname": f"eni-{uuid.uuid4().hex[:17]}",
            "service": "vpc",
            "status": status,
            "message": f"{src_ip}:{src_port} -> {dst_ip}:{dst_port} {protocol_name} {action} {packets}pkts {bytes_transferred}B",
            "network": {
                "protocol": protocol_name,
                "direction": "inbound" if is_inbound else "outbound",
                "source": {
                    "ip": src_ip,
                    "port": src_port,
                },
                "destination": {
                    "ip": dst_ip,
                    "port": dst_port,
                },
                "packets": packets,
                "bytes": bytes_transferred,
            },
            "vpc": {
                "action": action,
                "interface_id": f"eni-{uuid.uuid4().hex[:17]}",
                "subnet_id": f"subnet-{uuid.uuid4().hex[:17]}",
            },
        })
    
    return logs


def generate_dns_logs(count: int) -> list:
    """Generate DNS query logs."""
    logs = []
    
    domains = [
        # Internal
        "api.company.internal",
        "db.company.internal",
        "cache.company.internal",
        "auth.company.internal",
        
        # External services
        "api.stripe.com",
        "api.twilio.com",
        "api.sendgrid.com",
        "api.github.com",
        "s3.amazonaws.com",
        "sqs.us-east-1.amazonaws.com",
        
        # CDN
        "cdn.company.com",
        "static.company.com",
        
        # Suspicious
        "malware.evil.com",
        "c2.badactor.ru",
        "exfil.suspicious.cn",
    ]
    
    record_types = ["A", "AAAA", "CNAME", "MX", "TXT", "NS"]
    
    for _ in range(count):
        domain = random.choice(domains)
        record_type = random.choice(record_types)
        
        is_suspicious = "evil" in domain or "badactor" in domain or "suspicious" in domain
        
        if is_suspicious:
            response_code = random.choice(["NOERROR", "NXDOMAIN"])
            status = "warn"
        else:
            response_code = random.choices(
                ["NOERROR", "NXDOMAIN", "SERVFAIL"],
                weights=[90, 8, 2]
            )[0]
            status = "info" if response_code == "NOERROR" else "warn"
        
        query_time_ms = random.uniform(0.5, 50) if response_code == "NOERROR" else random.uniform(100, 1000)
        
        logs.append({
            "ddsource": "dns",
            "ddtags": f"env:production,service:dns,response_code:{response_code.lower()}",
            "hostname": random.choice(["ns1.company.internal", "ns2.company.internal"]),
            "service": "dns",
            "status": status,
            "message": f"DNS {record_type} query for {domain}: {response_code}",
            "dns": {
                "query": {
                    "name": domain,
                    "type": record_type,
                },
                "response_code": response_code,
                "query_time_ms": round(query_time_ms, 2),
            },
            "network": {
                "client": {
                    "ip": f"10.0.{random.randint(1, 10)}.{random.randint(1, 254)}",
                },
            },
        })
    
    return logs


def generate_incident_scenario_logs(count: int) -> list:
    """Generate a correlated incident scenario with cascading failures."""
    logs = []
    
    # Scenario: Database connection pool exhaustion causing service degradation
    incident_id = f"INC-{random.randint(10000, 99999)}"
    trace_id = generate_trace_id()
    
    # Phase 1: Initial database issues
    for _ in range(count // 4):
        logs.append({
            "ddsource": "postgresql",
            "ddtags": "env:production,service:postgres,incident:true",
            "hostname": "db-primary-01",
            "service": "postgres",
            "status": "warn",
            "message": f"Connection pool running low: {random.randint(1, 5)}/{50} available",
            "db": {
                "type": "postgresql",
                "operation": "connection_pool",
                "available_connections": random.randint(1, 5),
                "max_connections": 50,
            },
            "incident_id": incident_id,
        })
    
    # Phase 2: Application timeouts
    for _ in range(count // 4):
        service = random.choice(["user-service", "order-service", "payment-service"])
        logs.append({
            "ddsource": "python",
            "ddtags": f"env:production,service:{service},incident:true",
            "hostname": random.choice(HOSTS["api"]),
            "service": service,
            "status": "error",
            "message": f"Database connection timeout after 30s",
            "error": {
                "kind": "ConnectionTimeout",
                "message": "Timeout waiting for database connection from pool",
            },
            "trace_id": trace_id,
            "incident_id": incident_id,
            "duration": 30_000_000_000,  # 30 seconds
        })
    
    # Phase 3: Circuit breakers opening
    for _ in range(count // 4):
        service = random.choice(["api-gateway", "checkout-service"])
        logs.append({
            "ddsource": "go",
            "ddtags": f"env:production,service:{service},incident:true",
            "hostname": random.choice(HOSTS["api"]),
            "service": service,
            "status": "error",
            "message": "Circuit breaker OPEN for dependency: database-pool",
            "circuit_breaker": {
                "name": "database-pool",
                "state": "open",
                "failure_count": random.randint(10, 50),
                "failure_threshold": 10,
            },
            "incident_id": incident_id,
        })
    
    # Phase 4: User-facing errors
    for _ in range(count // 4):
        logs.append({
            "ddsource": "nginx",
            "ddtags": "env:production,service:api-gateway,incident:true",
            "hostname": random.choice(HOSTS["web"]),
            "service": "api-gateway",
            "status": "error",
            "message": f'{get_random_ip("residential")[0]} "POST /api/v1/checkout HTTP/1.1" 503',
            "http": {
                "method": "POST",
                "url": "/api/v1/checkout",
                "status_code": 503,
            },
            "incident_id": incident_id,
        })
    
    return logs


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def send_logs(logs: list, batch_size: int = 50) -> tuple[int, int]:
    """Send logs to Datadog in batches."""
    if not DD_API_KEY:
        print("❌ DD_API_KEY not set, logs not sent")
        return 0, len(logs)
    
    url = f"https://http-intake.logs.{DD_SITE}/api/v2/logs"
    headers = {
        "DD-API-KEY": DD_API_KEY,
        "Content-Type": "application/json",
    }
    
    success_count = 0
    error_count = 0
    total_batches = (len(logs) + batch_size - 1) // batch_size
    
    for i in range(0, len(logs), batch_size):
        batch = logs[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        try:
            response = requests.post(url, headers=headers, json=batch, timeout=30)
            if response.status_code == 202:
                success_count += len(batch)
                print(f"  Batch {batch_num}/{total_batches}: ✅ ({len(batch)} logs)")
            else:
                error_count += len(batch)
                print(f"  Batch {batch_num}/{total_batches}: ❌ Status {response.status_code}")
        except requests.RequestException as e:
            error_count += len(batch)
            print(f"  Batch {batch_num}/{total_batches}: ❌ Error: {e}")
        
        time.sleep(0.2)  # Rate limiting
    
    return success_count, error_count


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive log generator for Datadog",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python generate_logs_comprehensive.py                    # Generate ~1000 logs
    python generate_logs_comprehensive.py --count 500        # Generate ~500 logs
    python generate_logs_comprehensive.py --scenario incident # Generate incident scenario
    python generate_logs_comprehensive.py --duration 60      # Generate logs for 60 seconds
        """
    )
    parser.add_argument("--count", type=int, default=1000, help="Approximate number of logs to generate")
    parser.add_argument("--scenario", choices=["normal", "incident"], default="normal", help="Log scenario type")
    parser.add_argument("--duration", type=int, help="Generate logs continuously for N seconds")
    parser.add_argument("--dry-run", action="store_true", help="Generate logs but don't send to Datadog")
    args = parser.parse_args()
    
    if not DD_API_KEY and not args.dry_run:
        print("❌ Error: DD_API_KEY environment variable not set")
        print("   Run: export DD_API_KEY=your_api_key")
        print("   Or use --dry-run to generate without sending")
        return
    
    print(f"\n{'='*70}")
    print("🔍 Comprehensive Datadog Log Generator")
    print(f"{'='*70}")
    print(f"  Target: ~{args.count} logs")
    print(f"  Scenario: {args.scenario}")
    print(f"  Datadog Site: {DD_SITE}")
    print(f"  Dry Run: {args.dry_run}")
    print(f"{'='*70}\n")
    
    # Define generators with their weights
    generators = [
        ("HTTP Access Logs", generate_http_access_logs, 15),
        ("Application Logs", generate_application_logs, 20),
        ("Authentication Logs", generate_authentication_logs, 10),
        ("CloudTrail Logs", generate_cloudtrail_logs, 8),
        ("Database Logs", generate_database_logs, 8),
        ("Kubernetes Logs", generate_kubernetes_logs, 8),
        ("Kafka Logs", generate_kafka_logs, 5),
        ("Lambda Logs", generate_lambda_logs, 5),
        ("CI/CD Logs", generate_cicd_logs, 3),
        ("Payment Logs", generate_payment_logs, 5),
        ("CDN Logs", generate_cdn_logs, 5),
        ("WAF Logs", generate_waf_logs, 3),
        ("Load Balancer Logs", generate_load_balancer_logs, 5),
        ("Batch Job Logs", generate_batch_job_logs, 3),
        ("Audit Logs", generate_audit_logs, 3),
        ("Network Flow Logs", generate_network_flow_logs, 2),
        ("DNS Logs", generate_dns_logs, 2),
    ]
    
    if args.scenario == "incident":
        generators.append(("Incident Scenario Logs", generate_incident_scenario_logs, 10))
    
    total_weight = sum(g[2] for g in generators)
    
    def generate_batch(target_count: int) -> list:
        all_logs = []
        print("\n📝 Generating logs...\n")
        
        for name, generator, weight in generators:
            count = max(1, int(target_count * weight / total_weight))
            logs = generator(count)
            all_logs.extend(logs)
            print(f"  ✓ {name}: {len(logs)} logs")
        
        random.shuffle(all_logs)
        return all_logs
    
    if args.duration:
        print(f"\n⏱️  Generating logs for {args.duration} seconds...\n")
        start_time = time.time()
        total_sent = 0
        total_errors = 0
        
        while time.time() - start_time < args.duration:
            batch = generate_batch(args.count // 10)  # Smaller batches for continuous generation
            
            if not args.dry_run:
                success, errors = send_logs(batch)
                total_sent += success
                total_errors += errors
            else:
                total_sent += len(batch)
            
            elapsed = time.time() - start_time
            remaining = args.duration - elapsed
            if remaining > 0:
                print(f"\n  ⏳ {remaining:.0f}s remaining...\n")
                time.sleep(min(5, remaining))
        
        print(f"\n{'='*70}")
        print(f"✅ Continuous generation complete!")
        print(f"   Total logs sent: {total_sent}")
        if total_errors:
            print(f"   Errors: {total_errors}")
    else:
        all_logs = generate_batch(args.count)
        
        print(f"\n📊 Total logs generated: {len(all_logs)}\n")
        
        if args.dry_run:
            print("🔍 Dry run - logs not sent to Datadog")
            print(f"\n📄 Sample log:\n{json.dumps(all_logs[0], indent=2)}")
        else:
            print("📤 Sending logs to Datadog...\n")
            success, errors = send_logs(all_logs)
            
            print(f"\n{'='*70}")
            print(f"✅ Sent {success} logs to Datadog")
            if errors:
                print(f"❌ Failed to send {errors} logs")
    
    print(f"\n📝 Sample queries to test:")
    print("  • 'Show me errors from the payment service'")
    print("  • 'Failed login attempts from suspicious IPs'")
    print("  • 'AWS console logins from unusual locations'")
    print("  • 'Slow database queries over 5 seconds'")
    print("  • 'Kubernetes pod crashes in production'")
    print("  • 'WAF blocked requests'")
    print("  • 'Circuit breaker events'")
    print("  • 'Lambda cold starts and timeouts'")
    print("  • 'CI/CD pipeline failures'")
    print("  • 'Bulk data exports in audit logs'")
    print("  • '5xx errors from the API gateway'")
    print("  • 'Consumer lag in Kafka'")
    print("  • 'Database deadlocks'")
    print("  • 'S3 access denied errors in CloudTrail'")
    print("  • 'CDN cache misses'")
    if args.scenario == "incident":
        print("  • 'Show me all logs related to the incident'")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()