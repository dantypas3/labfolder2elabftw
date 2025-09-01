"""
Core package: orchestration of migration processes.
This package exposes the MigrationCoordinator class which ties together
the fetcher, importer and API clients to perform a migration.
"""

from .coordinator import Coordinator

__all__ = [
    "Coordinator",
]
