"""
Mock of the parts of the Teamleader API that the flow uses:

- POST /{resource}.list      paginated list, supports filter[updated_since]
- POST /{resource}.info      details of a single item
- POST /oauth2/access_token  refresh the access token

Like Teamleader, a refresh token can only be used once: every refresh replaces it.
Access tokens can be limited to a number of requests (see /_admin/settings), to make
them expire in the middle of a run.
The /_admin routes are only used by the tests to inspect and change the mock data.
"""

import uuid
from datetime import datetime, timedelta, timezone
from secrets import token_hex
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

CLIENT_ID = "dev-client"
CLIENT_SECRET = "dev-secret"
INITIAL_REFRESH_TOKEN = "dev-refresh-token"

ITEM_COUNTS = {
    "companies": 45,
    "contacts": 5,
    "invoices": 4,
    "departments": 2,
    "events": 3,
    "projects": 3,
    "users": 7,
    "customFieldDefinitions": 2,
}
SEED_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)

items: dict[str, dict[str, dict]] = {
    resource: {
        item_id: {
            "id": item_id,
            "name": f"{resource} {i}",
            "updated_at": SEED_TIME.isoformat(),
        }
        for i in range(count)
        for item_id in [str(uuid.uuid5(uuid.NAMESPACE_URL, f"{resource}/{i}"))]
    }
    for resource, count in ITEM_COUNTS.items()
}
# access token -> requests left, None means unlimited
access_tokens: dict[str, Optional[int]] = {}
refresh_tokens: set[str] = {INITIAL_REFRESH_TOKEN}
refresh_count = 0
access_token_max_uses: Optional[int] = None

app = FastAPI()


def respond(data) -> JSONResponse:
    reset = datetime.now(timezone.utc) + timedelta(minutes=1)
    return JSONResponse(
        {"data": data},
        headers={
            "X-RateLimit-Remaining": "100",
            "X-RateLimit-Reset": reset.isoformat(),
        },
    )


def check_access_token(request: Request):
    token = request.headers.get("Authorization", "").removeprefix("Bearer ")
    if token not in access_tokens:
        raise HTTPException(status_code=401, detail="Invalid access token")
    uses_left = access_tokens[token]
    if uses_left is not None:
        if uses_left == 1:
            del access_tokens[token]
        else:
            access_tokens[token] = uses_left - 1


def get_resource(resource: str) -> dict[str, dict]:
    if resource not in items:
        raise HTTPException(status_code=404, detail=f"Unknown resource {resource}")
    return items[resource]


@app.post("/{resource}.list")
async def list_items(resource: str, request: Request):
    check_access_token(request)
    form = await request.form()
    page = int(form.get("page[number]", 1))
    page_size = int(form.get("page[size]", 20))
    updated_since = form.get("filter[updated_since]")

    selected = sorted(get_resource(resource).values(), key=lambda item: item["id"])
    if updated_since:
        since = datetime.fromisoformat(updated_since)
        selected = [
            item
            for item in selected
            if datetime.fromisoformat(item["updated_at"]) > since
        ]

    start = (page - 1) * page_size
    page_items = selected[start : start + page_size]
    return respond([{"id": item["id"], "name": item["name"]} for item in page_items])


@app.post("/{resource}.info")
async def item_info(resource: str, request: Request):
    check_access_token(request)
    form = await request.form()
    item = get_resource(resource).get(str(form.get("id")))
    if item is None:
        raise HTTPException(status_code=404, detail="Unknown id")
    return respond(item)


@app.post("/oauth2/access_token")
async def access_token(request: Request):
    global refresh_count
    form = await request.form()
    if (
        form.get("grant_type") != "refresh_token"
        or form.get("client_id") != CLIENT_ID
        or form.get("client_secret") != CLIENT_SECRET
        or form.get("refresh_token") not in refresh_tokens
    ):
        raise HTTPException(status_code=400, detail="Invalid refresh request")

    new_access, new_refresh = token_hex(16), token_hex(16)
    access_tokens[new_access] = access_token_max_uses
    refresh_tokens.remove(form.get("refresh_token"))
    refresh_tokens.add(new_refresh)
    refresh_count += 1
    return {
        "token_type": "Bearer",
        "expires_in": 3600,
        "access_token": new_access,
        "refresh_token": new_refresh,
    }


@app.get("/_admin/health")
def health():
    return {"status": "ok"}


@app.get("/_admin/stats")
def stats():
    return {
        "item_counts": {resource: len(data) for resource, data in items.items()},
        "refresh_count": refresh_count,
    }


@app.post("/_admin/settings")
async def update_settings(request: Request):
    global access_token_max_uses
    access_token_max_uses = (await request.json())["access_token_max_uses"]


@app.post("/_admin/{resource}/{item_id}")
async def update_item(resource: str, item_id: str, request: Request):
    item = get_resource(resource)[item_id]
    item.update(await request.json())
    item["updated_at"] = datetime.now(timezone.utc).isoformat()
    return item
