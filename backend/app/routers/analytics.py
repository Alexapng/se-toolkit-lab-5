"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, distinct, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    lab_item = await _get_lab_item(session, lab)
    if lab_item is None:
        return _empty_score_buckets()

    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    statement = (
        select(bucket_case.label("bucket"), func.count(InteractionLog.id).label("count"))
        .select_from(InteractionLog)
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .where(
            ItemRecord.parent_id == lab_item.id,
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket_case)
    )
    result = await session.exec(statement)
    counts_by_bucket = {bucket: count for bucket, count in result.all()}

    return [
        {"bucket": bucket, "count": counts_by_bucket.get(bucket, 0)}
        for bucket in ["0-25", "26-50", "51-75", "76-100"]
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    lab_item = await _get_lab_item(session, lab)
    if lab_item is None:
        return []

    statement = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .select_from(ItemRecord)
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(
            ItemRecord.parent_id == lab_item.id,
            InteractionLog.score.is_not(None),
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    result = await session.exec(statement)

    return [
        {"task": task, "avg_score": avg_score, "attempts": attempts}
        for task, avg_score, attempts in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    lab_item = await _get_lab_item(session, lab)
    if lab_item is None:
        return []

    date_expr = func.date(InteractionLog.created_at)
    statement = (
        select(
            date_expr.label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .select_from(InteractionLog)
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_item.id)
        .group_by(date_expr)
        .order_by(date_expr)
    )
    result = await session.exec(statement)

    return [
        {"date": str(date_value), "submissions": submissions}
        for date_value, submissions in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    lab_item = await _get_lab_item(session, lab)
    if lab_item is None:
        return []

    statement = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(distinct(Learner.id)).label("students"),
        )
        .select_from(InteractionLog)
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(
            ItemRecord.parent_id == lab_item.id,
            InteractionLog.score.is_not(None),
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    result = await session.exec(statement)

    return [
        {"group": group, "avg_score": avg_score, "students": students}
        for group, avg_score, students in result.all()
    ]


async def _get_lab_item(session: AsyncSession, lab: str) -> ItemRecord | None:
    """Resolve a short lab id like `lab-04` to its lab item record."""
    lab_label = _lab_id_to_title_fragment(lab)
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_label),
    )
    result = await session.exec(statement)
    return result.first()


def _lab_id_to_title_fragment(lab: str) -> str:
    """Convert a lab short id to the title fragment stored in the database."""
    prefix, _, suffix = lab.partition("-")
    if prefix != "lab" or not suffix.isdigit():
        return lab
    return f"Lab {suffix}"


def _empty_score_buckets() -> list[dict[str, int | str]]:
    """Return the fixed score bucket list with zero counts."""
    return [
        {"bucket": "0-25", "count": 0},
        {"bucket": "26-50", "count": 0},
        {"bucket": "51-75", "count": 0},
        {"bucket": "76-100", "count": 0},
    ]
