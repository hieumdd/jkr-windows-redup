from redup.pipeline import opportunities

pipelines = {
    i.name: i
    for i in [
        j.pipeline
        for j in [
            opportunities,
        ]
    ]
}
