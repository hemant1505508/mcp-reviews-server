"""
mcp_server.py — MCP server exposing the Postgres reviews table to Snowflake.

Cloud-ready: all secrets come from environment variables.
Works locally too, if you set the same variables.

Required env vars:
    DATABASE_URL       postgres://user:pass@host:port/dbname
    OAUTH_CLIENT_ID    any string you invent
    OAUTH_CLIENT_SECRET any string you invent

Run locally:
    set DATABASE_URL=postgresql://postgres:<password>@localhost:5432/Superstore
    set OAUTH_CLIENT_ID=snowflake-demo-client
    set OAUTH_CLIENT_SECRET=snowflake-demo-secret
    python -m uvicorn mcp_server:app --host 0.0.0.0 --port 8000
"""

import base64
import json
import logging
import os
import secrets
from urllib.parse import unquote_plus, urlencode

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse

DATABASE_URL = os.environ.get("DATABASE_URL", "")
CLIENT_ID = os.environ.get("OAUTH_CLIENT_ID", "snowflake-demo-client")
CLIENT_SECRET = os.environ.get("OAUTH_CLIENT_SECRET", "change-me")
TABLE = os.environ.get("TABLE_NAME", "reviews")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mcp_server")

app = FastAPI(title="Postgres Reviews MCP")

_codes = {}
_tokens = {}

TOOLS = [
    {
        "name": "query_reviews",
        "description": (
            "Query customer reviews from an external Postgres database "
            "(Superstore) that is NOT part of Snowflake. Each review has "
            "review_id, entity_id (the product/project/user being reviewed), "
            "reviewer_name, reviewer_email, rating (1-5), review_text, "
            "status (Pending, Approved or Rejected), created_at and updated_at. "
            "Returns matching rows plus review count, average rating and "
            "counts by status and by rating. Use this tool for ANY question "
            "about reviews, ratings, feedback or customer sentiment."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity_id": {
                    "type": "integer",
                    "description": "ID of the reviewed entity, e.g. 101. Omit for all entities.",
                },
                "status": {
                    "type": "string",
                    "description": "Pending, Approved or Rejected. Omit for all statuses.",
                },
                "min_rating": {
                    "type": "integer",
                    "description": "Lowest rating to include, 1-5.",
                },
                "max_rating": {
                    "type": "integer",
                    "description": "Highest rating to include, 1-5.",
                },
                "reviewer_name": {
                    "type": "string",
                    "description": "Case-insensitive partial match on reviewer name.",
                },
                "text_contains": {
                    "type": "string",
                    "description": "Case-insensitive keyword to search for in review_text, e.g. delivery.",
                },
            },
        },
    }
]


def run_query(args):
    if not DATABASE_URL:
        return {"error": "DATABASE_URL not set on the server"}

    where, params = [], []
    if args.get("entity_id") not in (None, ""):
        where.append("entity_id = %s")
        params.append(int(args["entity_id"]))
    if args.get("status"):
        where.append("LOWER(status) = LOWER(%s)")
        params.append(args["status"])
    if args.get("min_rating") not in (None, ""):
        where.append("rating >= %s")
        params.append(int(args["min_rating"]))
    if args.get("max_rating") not in (None, ""):
        where.append("rating <= %s")
        params.append(int(args["max_rating"]))
    if args.get("reviewer_name"):
        where.append("reviewer_name ILIKE %s")
        params.append(f"%{args['reviewer_name']}%")
    if args.get("text_contains"):
        where.append("review_text ILIKE %s")
        params.append(f"%{args['text_contains']}%")

    sql = f"SELECT * FROM {TABLE}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY review_id LIMIT 3000"

    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    # a small aggregate helps the agent reason without re-counting every row
    by_status, by_rating, ratings = {}, {}, []
    for r in rows:
        s = r.get("status") or "unknown"
        by_status[s] = by_status.get(s, 0) + 1
        if r.get("rating") is not None:
            ratings.append(r["rating"])
            k = str(r["rating"])
            by_rating[k] = by_rating.get(k, 0) + 1

    return {
        "source": "external Postgres (not Snowflake)",
        "sql": sql,
        "row_count": len(rows),
        "average_rating": round(sum(ratings) / len(ratings), 2) if ratings else None,
        "count_by_status": by_status,
        "count_by_rating": by_rating,
        "rows": rows,
    }


# ======================= OAuth =======================
@app.get("/oauth/authorize")
def authorize(client_id: str = "", redirect_uri: str = "", state: str = "",
              scope: str = "", response_type: str = "code"):
    if client_id != CLIENT_ID:
        raise HTTPException(400, "bad client_id")
    code = secrets.token_urlsafe(24)
    _codes[code] = True
    sep = "&" if "?" in redirect_uri else "?"
    # state is opaque to us and may contain + / = &, so it must be re-encoded
    query = urlencode({"code": code, "state": state})
    log.info("authorize: redirecting to %s", redirect_uri)
    return RedirectResponse(f"{redirect_uri}{sep}{query}", status_code=302)


@app.post("/oauth/token")
async def token(request: Request):
    try:
        form = await request.form()
    except Exception:
        form = {}

    auth = request.headers.get("authorization", "")
    cid = form.get("client_id")
    csec = form.get("client_secret")

    if auth.lower().startswith("basic "):
        try:
            decoded = base64.b64decode(auth[6:]).decode()
            cid, csec = decoded.split(":", 1)
            # RFC 6749 2.3.1: clients may form-urlencode id and secret
            if (cid, csec) != (CLIENT_ID, CLIENT_SECRET):
                cid, csec = unquote_plus(cid), unquote_plus(csec)
        except Exception:
            pass

    log.info("token: grant_type=%s client_id=%s basic_auth=%s",
             form.get("grant_type"), cid, auth.lower().startswith("basic "))
    if cid != CLIENT_ID or csec != CLIENT_SECRET:
        log.warning("token: rejected - client id/secret mismatch")
        raise HTTPException(401, "bad client credentials")

    tok = secrets.token_urlsafe(32)
    _tokens[tok] = True
    return {
        "access_token": tok,
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": secrets.token_urlsafe(32),
        "scope": form.get("scope") or "read",
    }


# ======================= MCP =======================
@app.post("/mcp")
async def mcp(request: Request):
    body = await request.json()
    method = body.get("method")
    rid = body.get("id")

    if method == "initialize":
        result = {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "postgres-reviews-mcp", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        p = body.get("params", {})
        if p.get("name") == "query_reviews":
            data = run_query(p.get("arguments") or {})
            result = {"content": [{"type": "text",
                                   "text": json.dumps(data, default=str)}]}
        else:
            result = {"content": [{"type": "text",
                                   "text": f"unknown tool {p.get('name')}"}],
                      "isError": True}
    elif method in ("notifications/initialized", "ping"):
        result = {}
    else:
        return {"jsonrpc": "2.0", "id": rid,
                "error": {"code": -32601, "message": f"unknown method {method}"}}

    return {"jsonrpc": "2.0", "id": rid, "result": result}


@app.get("/health")
def health():
    db_ok = False
    detail = "DATABASE_URL not set"
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            n = cur.fetchone()[0]
            cur.close()
            conn.close()
            db_ok = True
            detail = f"{n} rows in {TABLE}"
        except Exception as e:
            detail = str(e)[:200]
    return {"ok": True, "database": db_ok, "detail": detail}


@app.get("/")
def root():
    return {"service": "postgres-reviews-mcp",
            "endpoints": ["/health", "/mcp", "/oauth/authorize", "/oauth/token"]}
