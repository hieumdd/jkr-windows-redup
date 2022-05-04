from typing import Optional
from datetime import datetime, timedelta

from redup.pipeline.interface import Pipeline
from redup.redup_repo import get_listing

TIME_FORMAT = "%Y-%m-%d"


def get_timestamp(input_: Optional[str], fallback: datetime):
    dt = datetime.strptime(input_, "%Y-%m-%d") if input_ else fallback
    return int(dt.timestamp()) * 1e3


pipeline = Pipeline(
    "Opportunities",
    lambda body: {
        "startDate": get_timestamp(
            body.get("start"),
            (datetime.utcnow() - timedelta(days=30)),
        ),
        "endDate": get_timestamp(body.get("end"), datetime.utcnow()),
    },
    get_listing(
        "pipelines/wgK9atPjZzl6KwZFjYPD/opportunities",
        lambda x: x["opportunities"],
        lambda x: {
            "startAfterId": x["meta"]["startAfterId"],
            "startAfter": x["meta"]["startAfter"],
        },
    ),
    lambda rows: [
        {
            "id": row.get("id"),
            "name": row.get("name"),
            "monetaryValue": row.get("monetaryValue"),
            "pipelineId": row.get("pipelineId"),
            "pipelineStageId": row.get("pipelineStageId"),
            "pipelineStageUId": row.get("pipelineStageUId"),
            "assignedTo": row.get("assignedTo"),
            "status": row.get("status"),
            "source": row.get("source"),
            "lastStatusChangeAt": row.get("lastStatusChangeAt"),
            "createdAt": row.get("createdAt"),
            "updatedAt": row.get("updatedAt"),
            "contact": {
                "id": row["contact"].get("id"),
                "name": row["contact"].get("name"),
                "email": row["contact"].get("email"),
                "phone": row["contact"].get("phone"),
                "tags": [i for i in row["contact"].get("tags", [])],
            }
            if row.get("contact")
            else {},
        }
        for row in rows
    ],
    [
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "monetaryValue", "type": "NUMERIC"},
        {"name": "pipelineId", "type": "STRING"},
        {"name": "pipelineStageId", "type": "STRING"},
        {"name": "pipelineStageUId", "type": "STRING"},
        {"name": "assignedTo", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "lastStatusChangeAt", "type": "TIMESTAMP"},
        {"name": "createdAt", "type": "TIMESTAMP"},
        {"name": "updatedAt", "type": "TIMESTAMP"},
        {
            "name": "contact",
            "type": "RECORD",
            "fields": [
                {"name": "id", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "email", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
                {"name": "tags", "type": "STRING", "mode": "REPEATED"},
            ],
        },
    ],
    cursor_key="createdAt",
)
