"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime, timezone

import httpx
from sqlalchemy import func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=httpx.BasicAuth(
                username=settings.autochecker_email,
                password=settings.autochecker_password,
            ),
        )
    response.raise_for_status()
    return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs: list[dict] = []
    next_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if next_since is not None:
                params["since"] = next_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=httpx.BasicAuth(
                    username=settings.autochecker_email,
                    password=settings.autochecker_password,
                ),
            )
            response.raise_for_status()

            payload = response.json()
            page_logs = payload["logs"]
            all_logs.extend(page_logs)

            if not payload["has_more"] or not page_logs:
                break

            next_since = _parse_timestamp(page_logs[-1]["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    created_count = 0
    labs_by_short_id: dict[str, ItemRecord] = {}

    for item in items:
        if item["type"] != "lab":
            continue

        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == item["title"],
        )
        result = await session.exec(statement)
        lab_record = result.first()

        if lab_record is None:
            lab_record = ItemRecord(type="lab", title=item["title"])
            session.add(lab_record)
            await session.flush()
            created_count += 1

        labs_by_short_id[item["lab"]] = lab_record

    for item in items:
        if item["type"] != "task":
            continue

        parent_lab = labs_by_short_id.get(item["lab"])
        if parent_lab is None:
            continue

        statement = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == item["title"],
            ItemRecord.parent_id == parent_lab.id,
        )
        result = await session.exec(statement)
        task_record = result.first()

        if task_record is not None:
            continue

        session.add(
            ItemRecord(
                type="task",
                title=item["title"],
                parent_id=parent_lab.id,
            )
        )
        created_count += 1

    await session.commit()
    return created_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    created_count = 0
    item_titles_by_short_id: dict[tuple[str, str | None], str] = {}

    for item in items_catalog:
        item_key = (item["lab"], item["task"])
        item_titles_by_short_id[item_key] = item["title"]

    for log in logs:
        learner_statement = select(Learner).where(
            Learner.external_id == log["student_id"]
        )
        learner_result = await session.exec(learner_statement)
        learner = learner_result.first()

        if learner is None:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log["group"],
            )
            session.add(learner)
            await session.flush()

        item_title = item_titles_by_short_id.get((log["lab"], log["task"]))
        if item_title is None:
            continue

        item_statement = select(ItemRecord).where(ItemRecord.title == item_title)
        item_result = await session.exec(item_statement)
        item = item_result.first()
        if item is None:
            continue

        interaction_statement = select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
        interaction_result = await session.exec(interaction_statement)
        existing_interaction = interaction_result.first()
        if existing_interaction is not None:
            continue

        session.add(
            InteractionLog(
                external_id=log["id"],
                learner_id=learner.id,
                item_id=item.id,
                kind="attempt",
                score=log["score"],
                checks_passed=log["passed"],
                checks_total=log["total"],
                created_at=_parse_timestamp(log["submitted_at"]),
            )
        )
        created_count += 1

    await session.commit()
    return created_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    latest_timestamp_statement = select(func.max(InteractionLog.created_at))
    latest_timestamp_result = await session.exec(latest_timestamp_statement)
    since = latest_timestamp_result.one()

    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items_catalog, session)

    total_records_statement = select(func.count()).select_from(InteractionLog)
    total_records_result = await session.exec(total_records_statement)
    total_records = total_records_result.one()

    return {"new_records": new_records, "total_records": total_records}


def _parse_timestamp(timestamp: str) -> datetime:
    """Parse an ISO timestamp and normalize it to a naive UTC datetime."""
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed
