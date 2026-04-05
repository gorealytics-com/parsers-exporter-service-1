"""delivery-exporter: Validates delivery organisation data.

Same validation logic as org-exporter (OrgValidator) but deployed as separate
Deployment type for delivery sources (source_23..source_61).

Queue pattern: delivery:source_XX:items
Kafka topic: orgs.validated.v1

The exporter code is IDENTICAL to org-exporter — the separation is for:
  - Independent HPA scaling (delivery sources are higher volume)
  - Separate resource profiles (large sources like ubereats/swiggy)
  - Operational isolation (delivery issues don't affect map exporters)
"""

import asyncio
import os
import sys

sys.path.insert(0, "/app")
from org_exporter.main import OrgValidator

from exporter_base.base import ExporterConfig, ExporterRunner, setup_logging


async def main() -> None:
    """Start the delivery-exporter service."""
    setup_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = ExporterConfig.from_env()
    validator = OrgValidator(config)
    runner = ExporterRunner(validator, config)
    await runner.start()


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.run(main())
    except ImportError:
        asyncio.run(main())
