from redup.pipeline import pipelines
from redup import redup_service


def redup_controller(body: dict[str, str]):
    return redup_service.pipeline_service(
        pipelines[body.get("table", "")],
        body
    )
