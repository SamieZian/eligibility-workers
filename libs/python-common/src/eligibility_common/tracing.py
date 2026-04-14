"""OpenTelemetry bootstrap.

In GCP (when ``GOOGLE_CLOUD_PROJECT`` is set and the
``opentelemetry-exporter-gcp-trace`` extra is installed), spans go to
**Cloud Trace**. Elsewhere, they go to the OTLP endpoint (Jaeger locally).
No-op if neither is configured.
"""
from __future__ import annotations

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def init_tracing(service_name: str) -> None:
    gcp_project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    if not gcp_project and not otlp_endpoint:
        return

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": os.environ.get("SERVICE_VERSION", "0.1.0"),
        }
    )
    provider = TracerProvider(resource=resource)

    if gcp_project:
        try:
            from opentelemetry.exporter.cloud_trace import (  # type: ignore[import-not-found]
                CloudTraceSpanExporter,
            )

            provider.add_span_processor(
                BatchSpanProcessor(CloudTraceSpanExporter(project_id=gcp_project))
            )
        except ImportError:
            # GCP exporter not installed — fall through to OTLP if available.
            if otlp_endpoint:
                provider.add_span_processor(
                    BatchSpanProcessor(
                        OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
                    )
                )
    elif otlp_endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
        )

    trace.set_tracer_provider(provider)


def tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)
