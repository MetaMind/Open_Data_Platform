"""{{ cookiecutter.pipeline_name }} — streaming pipeline (Flink/Kafka)."""
from odep.sdk.pipeline import Pipeline

pipeline = Pipeline(
    name="{{ cookiecutter.pipeline_name }}",
    description="{{ cookiecutter.description }}",
    sources=[
        {"urn": "urn:li:dataset:(kafka,{{ cookiecutter.kafka_topic }},dev)", "name": "kafka_source"}
    ],
    sinks=[
        {"urn": "urn:li:dataset:(flink,{{ cookiecutter.pipeline_name }}.output,dev)", "name": "flink_sink"}
    ],
    transforms=[
        {"name": "stream_transform", "sql": "SELECT * FROM kafka_source"}
    ],
)
