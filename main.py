
import base64
import json
import math
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Lock

from fastapi import FastAPI, Header, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="Orders API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotency-Replayed"],
)

TOTAL_ORDERS = 40
RATE_LIMIT = 18
WINDOW_SECONDS = 10

# Fixed catalog, IDs 1 through 40.
catalog = [
    {"id": i, "status": "available", "description": f"Order {i}"}
    for i in range(1, TOTAL_ORDERS + 1)
]

# In-memory idempotency store and rate-limit buckets.
idempotency_store = {}
client_requests = defaultdict(deque)
lock = Lock()


@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    # Browsers send preflight requests before cross-origin requests.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "anonymous")
    now = time.monotonic()

    with lock:
        bucket = client_requests[client_id]

        while bucket and now - bucket[0] >= WINDOW_SECONDS:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT:
            retry_after = max(
                1, math.ceil(WINDOW_SECONDS - (now - bucket[0]))
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        bucket.append(now)

    return await call_next(request)


@app.post("/orders", status_code=201)
async def create_order(
    idempotency_key: str = Header(
        default=None, alias="Idempotency-Key"
    ),
):
    if not idempotency_key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header is required",
        )

    with lock:
        if idempotency_key in idempotency_store:
            order = idempotency_store[idempotency_key]
            return JSONResponse(
                status_code=200,
                content=order,
                headers={"Idempotency-Replayed": "true"},
            )

        order = {
            "id": str(uuid.uuid4()),
            "status": "created",
            "idempotency_key": idempotency_key,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        idempotency_store[idempotency_key] = order

    return JSONResponse(status_code=201, content=order)


@app.get("/orders")
async def list_orders(
    limit: int = Query(default=10, ge=1, le=40),
    cursor: str = Query(default=None),
):
    # Cursor is base64-encoded offset; callers pass it back unchanged.
    offset = 0

    if cursor:
        try:
            decoded = base64.urlsafe_b64decode(
                cursor + "=" * (-len(cursor) % 4)
            ).decode()
            offset = int(decoded)
            if offset < 0 or offset > TOTAL_ORDERS:
                raise ValueError("Invalid offset")
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor")

    items = catalog[offset:offset + limit]
    next_offset = offset + len(items)

    next_cursor = None
    if next_offset < TOTAL_ORDERS:
        next_cursor = base64.urlsafe_b64encode(
            str(next_offset).encode()
        ).decode().rstrip("=")

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/")
async def root():
    return {"service": "orders-api", "total_orders": TOTAL_ORDERS}
