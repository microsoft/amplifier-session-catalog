"""Public catalog API. Native files are always read-only."""

from .catalog import Catalog, InvalidCursor

__all__ = ["Catalog", "InvalidCursor"]
