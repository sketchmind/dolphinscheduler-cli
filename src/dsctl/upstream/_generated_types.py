from __future__ import annotations

from typing import TypeAlias

# Exact generated packages expose a different set of Pydantic models, operation
# groups, and result types for every DolphinScheduler release.  Those values are
# intentionally opaque only while crossing the dynamic import seam; adapters
# must validate their concrete shape before projecting them into stable domain
# protocols.  JSON payloads must continue to use JsonValue/JsonObject instead.
OpaqueGeneratedValue: TypeAlias = object
