"""sitemap-exporter: Validates sitemap-sourced delivery organisation data.

Same validation logic as org-exporter (OrgValidator) but deployed for
sitemap crawler sources (delivery:source_XX:sitemap:items).

Queue pattern: delivery:source_XX:sitemap:items
Kafka topic: orgs.validated.v1

Separated from delivery-exporter because:
  - Sitemap data has different volume patterns (bulk periodic)
  - May need different batch sizes and check intervals
  - Operational isolation for debugging sitemap-specific issues
"""

import asyncio
import os
import sys

sys.path.insert(0, "/app")
from org_exporter.main import OrgValidator

from exporter_base.base import ExporterConfig, ExporterRunner, setup_logging


async def main() -> None:
    """Start the sitemap-exporter service."""
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
