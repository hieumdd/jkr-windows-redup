from typing import Union, Any

from compose import compose

from redup.pipeline.interface import Pipeline
from db.bigquery import load, update


def pipeline_service(
    pipeline: Pipeline,
    body: dict[str, Any],
) -> dict[str, Union[str, int]]:
    return compose(
        lambda x: {
            "table": pipeline.name,
            "start": body.get("start"),
            "end": body.get("end"),
            "output_rows": x,
        },
        load(
            pipeline.name,
            pipeline.schema,
            update(pipeline.id_key, pipeline.cursor_key),
        ),
        pipeline.transform,
        pipeline.get,
        pipeline.params_fn,
    )(body)
