"""
mcp_server.py — MCP server exposing the Postgres reviews table and the promo pamphlet
vector store (Supabase marketing schema) to Snowflake.

Cloud-ready: all secrets come from environment variables.
Works locally too, if you set the same variables.

Required env vars:
    DATABASE_URL       postgres://user:pass@host:port/dbname
    OAUTH_CLIENT_ID    any string you invent
    OAUTH_CLIENT_SECRET any string you invent
    SUPABASE_URL       https://<ref>.supabase.co   (for search_pamphlets)
    SUPABASE_ANON_KEY  Supabase legacy anon key    (for search_pamphlets)

Run locally:
    set DATABASE_URL=postgresql://postgres:<password>@localhost:5432/Superstore
    set OAUTH_CLIENT_ID=snowflake-demo-client
    set OAUTH_CLIENT_SECRET=snowflake-demo-secret
    set SUPABASE_URL=https://zgsypgzjtnzvlfdczrtp.supabase.co
    set SUPABASE_ANON_KEY=<anon key>
    python -m uvicorn mcp_server:app --host 0.0.0.0 --port 8000
"""

import base64
import json
import logging
import os
import secrets
import urllib.error
import urllib.request
from urllib.parse import unquote_plus, urlencode

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse

DATABASE_URL = os.environ.get("DATABASE_URL", "")
CLIENT_ID = os.environ.get("OAUTH_CLIENT_ID", "snowflake-demo-client")
CLIENT_SECRET = os.environ.get("OAUTH_CLIENT_SECRET", "change-me")
TABLE = os.environ.get("TABLE_NAME", "reviews")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

PAMPHLET_REGIONS = ["United States", "Asia Pacific", "EMEA", "Europe", "Latin America", "Africa"]

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mcp_server")

app = FastAPI(title="Postgres Reviews MCP")

_codes = {}
_tokens = {}

TOOLS = [
    {
        "name": "query_reviews",
        "description": (
            "Query Superstore customer product reviews from an external Postgres database "
            "(Supabase) that is NOT part of Snowflake. Each review has review_id, "
            "reviewer_ref (pseudonymous reviewer id), order_number (Superstore order id, "
            "e.g. CA-2011-130428; may be null), product_sku (Superstore product id, e.g. "
            "FUR-CH-10002965), product_title, rating (1-5), title, body (review text), "
            "created_at, verified_purchase (true/false) and updated_at. "
            "Returns matching rows plus review count, average rating and counts by rating, "
            "by verified_purchase and by product_sku. Use this tool for ANY question about "
            "reviews, ratings, feedback or customer sentiment. product_sku joins to "
            "Snowflake order/product data and to pamphlet offers (get_pamphlet_offers)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "product_sku": {
                    "type": "array", "items": {"type": "string"},
                    "description": "One or more exact product SKUs, e.g. [\"FUR-CH-10002965\"]. Omit for all products.",
                },
                "product_title_contains": {
                    "type": "string",
                    "description": "Case-insensitive partial match on product_title, e.g. chair.",
                },
                "order_number": {
                    "type": "string",
                    "description": "Exact Superstore order number, e.g. CA-2011-130428.",
                },
                "reviewer_ref": {
                    "type": "string",
                    "description": "Exact reviewer reference, e.g. kn-164501@customers.example.",
                },
                "min_rating": {
                    "type": "integer",
                    "description": "Lowest rating to include, 1-5.",
                },
                "max_rating": {
                    "type": "integer",
                    "description": "Highest rating to include, 1-5.",
                },
                "verified_purchase": {
                    "type": "boolean",
                    "description": "true = only verified purchases, false = only unverified. Omit for both.",
                },
                "created_from": {
                    "type": "string",
                    "description": "ISO date (YYYY-MM-DD); only reviews created on or after this day.",
                },
                "created_to": {
                    "type": "string",
                    "description": "ISO date (YYYY-MM-DD); only reviews created on or before this day.",
                },
                "text_contains": {
                    "type": "string",
                    "description": "Case-insensitive keyword searched in review title and body, e.g. delivery.",
                },
            },
        },
    },
    {
        "name": "search_pamphlets",
        "description": (
            "Semantic (vector) search over Superstore promotional pamphlets / marketing "
            "campaigns from 2011-2014, stored outside Snowflake. Use for questions about "
            "promotions, discounts, sales, coupons or campaigns described in natural "
            "language, e.g. 'printer discounts' or 'furniture sales in Latin America'. "
            "Each match has campaign_code, chunk_type ('offer' = one discounted product, "
            "'pamphlet' = whole campaign summary), region, valid_from, valid_to, "
            "product_sku, product_title, discount_pct, content and similarity (0-1, higher "
            "is closer). product_sku matches Superstore product IDs; for exact SKU, region "
            "or date lookups use get_pamphlet_offers instead."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to look for, in plain language."},
                "match_count": {"type": "integer", "description": "Number of results, 1-50. Default 5."},
                "filter_type": {
                    "type": "string", "enum": ["offer", "pamphlet"],
                    "description": "'offer' for individual product discounts, 'pamphlet' for whole campaigns. Omit for both.",
                },
                "filter_region": {
                    "type": "string", "enum": PAMPHLET_REGIONS,
                    "description": "Restrict to one campaign region. Omit for all regions.",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_pamphlet_offers",
        "description": (
            "Exact lookup of product discount offers from Superstore promotional pamphlets "
            "(2011-2014), stored outside Snowflake. Filter by product SKU, campaign code, "
            "region or a date that falls inside the campaign's validity window. Returns one "
            "row per product offer: campaign_code, campaign_name, region, valid_from, "
            "valid_to, product_sku, product_title, category, discount_pct. Use to check "
            "whether/when a product was on promotion, e.g. to compare with review dates."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "product_sku": {"type": "string", "description": "Exact product SKU, e.g. FUR-CH-10002965."},
                "campaign_code": {"type": "string", "description": "Exact campaign code, e.g. CMP-2012-EMEA-09."},
                "region": {"type": "string", "enum": PAMPHLET_REGIONS, "description": "Campaign region."},
                "active_on": {"type": "string", "description": "ISO date (YYYY-MM-DD); return offers valid on that day."},
                "category": {
                    "type": "string", "enum": ["Furniture", "Office Supplies", "Technology"],
                    "description": "Product category.",
                },
            },
        },
    },
]


def run_query(args):
    if not DATABASE_URL:
        return {"error": "DATABASE_URL not set on the server"}

    where, params = [], []
    skus = args.get("product_sku")
    if isinstance(skus, str):
        skus = [s for s in skus.split(",")]
    skus = [s.strip().upper() for s in (skus or []) if s and s.strip()]
    if skus:
        where.append("product_sku = ANY(%s)")
        params.append(skus)
    if args.get("product_title_contains"):
        where.append("product_title ILIKE %s")
        params.append(f"%{args['product_title_contains']}%")
    if args.get("order_number"):
        where.append("order_number = %s")
        params.append(args["order_number"].strip().upper())
    if args.get("reviewer_ref"):
        where.append("reviewer_ref = %s")
        params.append(args["reviewer_ref"].strip())
    if args.get("min_rating") not in (None, ""):
        where.append("rating >= %s")
        params.append(int(args["min_rating"]))
    if args.get("max_rating") not in (None, ""):
        where.append("rating <= %s")
        params.append(int(args["max_rating"]))
    if args.get("verified_purchase") not in (None, ""):
        v = args["verified_purchase"]
        where.append("verified_purchase = %s")
        params.append(v if isinstance(v, bool) else str(v).strip().lower() in ("true", "1", "yes"))
    if args.get("created_from"):
        where.append("created_at >= %s::date")
        params.append(args["created_from"])
    if args.get("created_to"):
        where.append("created_at < %s::date + 1")
        params.append(args["created_to"])
    if args.get("text_contains"):
        where.append("(title ILIKE %s OR body ILIKE %s)")
        params += [f"%{args['text_contains']}%"] * 2

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
    by_verified, by_rating, by_sku, ratings = {}, {}, {}, []
    for r in rows:
        v = str(r.get("verified_purchase")).lower()
        by_verified[v] = by_verified.get(v, 0) + 1
        sku = r.get("product_sku") or "unknown"
        by_sku[sku] = by_sku.get(sku, 0) + 1
        if r.get("rating") is not None:
            ratings.append(r["rating"])
            k = str(r["rating"])
            by_rating[k] = by_rating.get(k, 0) + 1

    return {
        "source": "external Postgres (not Snowflake)",
        "sql": sql,
        "row_count": len(rows),
        "average_rating": round(sum(ratings) / len(ratings), 2) if ratings else None,
        "count_by_rating": by_rating,
        "count_by_verified_purchase": by_verified,
        "count_by_product_sku": by_sku,
        "rows": rows,
    }


def search_pamphlets(args):
    # The query must be embedded with the same model as the stored vectors (gte-small),
    # which the Supabase `pamphlets` edge function does before running the vector search.
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return {"error": "SUPABASE_URL / SUPABASE_ANON_KEY not set on the server"}
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    payload = {
        "action": "search",
        "query": query,
        "match_count": int(args.get("match_count") or 5),
        "filter_type": args.get("filter_type") or None,
        "filter_region": args.get("filter_region") or None,
    }
    req = urllib.request.Request(
        f"{SUPABASE_URL}/functions/v1/pamphlets",
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {SUPABASE_ANON_KEY}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            matches = json.load(r)["matches"]
    except urllib.error.HTTPError as e:
        return {"error": f"pamphlet search failed: HTTP {e.code} {e.read().decode(errors='replace')[:300]}"}
    except urllib.error.URLError as e:
        return {"error": f"pamphlet search failed: {e.reason}"}
    return {
        "source": "Supabase vector store marketing.pamphlet_chunks (not Snowflake)",
        "query": query,
        "match_count": len(matches),
        "matches": matches,
    }


def get_pamphlet_offers(args):
    if not DATABASE_URL:
        return {"error": "DATABASE_URL not set on the server"}

    where, params = ["c.chunk_type = 'offer'"], []
    if args.get("product_sku"):
        where.append("c.product_sku = %s")
        params.append(args["product_sku"].strip().upper())
    if args.get("campaign_code"):
        where.append("c.campaign_code = %s")
        params.append(args["campaign_code"].strip().upper())
    if args.get("region"):
        where.append("p.region = %s")
        params.append(args["region"])
    if args.get("category"):
        where.append("c.category = %s")
        params.append(args["category"])
    if args.get("active_on"):
        where.append("%s::date BETWEEN p.valid_from AND p.valid_to")
        params.append(args["active_on"])

    sql = (
        "SELECT c.campaign_code, p.campaign_name, p.region, p.valid_from, p.valid_to, "
        "c.product_sku, c.product_title, c.category, c.discount_pct "
        "FROM marketing.pamphlet_chunks c JOIN marketing.pamphlets p USING (campaign_code) "
        "WHERE " + " AND ".join(where) + " ORDER BY p.valid_from, c.campaign_code, c.product_sku LIMIT 500"
    )

    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    return {
        "source": "external Postgres marketing.pamphlets (not Snowflake)",
        "sql": sql,
        "row_count": len(rows),
        "rows": rows,
    }


TOOL_HANDLERS = {
    "query_reviews": run_query,
    "search_pamphlets": search_pamphlets,
    "get_pamphlet_offers": get_pamphlet_offers,
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
            "serverInfo": {"name": "postgres-reviews-mcp", "version": "1.1.0"},
        }
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        p = body.get("params", {})
        handler = TOOL_HANDLERS.get(p.get("name"))
        if handler:
            try:
                data = handler(p.get("arguments") or {})
            except Exception as e:
                log.exception("tool %s failed", p.get("name"))
                data = {"error": str(e)[:300]}
            result = {"content": [{"type": "text",
                                   "text": json.dumps(data, default=str)}]}
            if "error" in data:
                result["isError"] = True
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
            cur.execute("SELECT COUNT(*) FROM marketing.pamphlet_chunks")
            m = cur.fetchone()[0]
            cur.close()
            conn.close()
            db_ok = True
            detail = f"{n} rows in {TABLE}, {m} pamphlet chunks"
        except Exception as e:
            detail = str(e)[:200]
    return {"ok": True, "database": db_ok, "detail": detail,
            "pamphlet_search_configured": bool(SUPABASE_URL and SUPABASE_ANON_KEY)}


@app.get("/")
def root():
    return {"service": "postgres-reviews-mcp",
            "endpoints": ["/health", "/mcp", "/oauth/authorize", "/oauth/token"]}
