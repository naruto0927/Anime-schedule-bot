"""
utils/filters.py — ChatFilter: per-chat schedule filter model.

Controls air types (raw/sub/dub), media types, streaming platforms,
and the hide-donghua toggle. Loaded from MongoDB per chat/topic;
serialised via to_dict() and restored via from_doc().
"""

from __future__ import annotations
from typing import Optional
from config import MEDIA_TYPES, STREAMS

class ChatFilter:
    """
    Per-chat schedule filter.  Stored inside the existing chats collection.

    Filters matching animeschedule.net's own filter panel:
        air_types    — set of enabled air types: {"raw","sub","dub"} (default: all)
        media_types  — set of enabled media type slugs (default: all = empty set means no filter)
        streams      — set of required streaming platform slugs (default: any)
        hide_donghua — exclude Chinese anime (default: False)
    """
    def __init__(
        self,
        air_types:    Optional[set] = None,
        media_types:  Optional[set] = None,
        streams:      Optional[set] = None,
        hide_donghua: bool = False,
    ):
        # None/empty means "show all"
        self.air_types    = set(air_types)   if air_types    else {"raw", "sub", "dub"}
        self.media_types  = set(media_types) if media_types  else set()
        self.streams      = set(streams)     if streams      else set()
        self.hide_donghua = hide_donghua

    @classmethod
    def from_doc(cls, doc: Optional[dict]) -> "ChatFilter":
        if not doc:
            return cls()
        f = doc.get("filter", {})
        return cls(
            air_types    = set(f.get("air_types",   ["raw", "sub", "dub"])),
            media_types  = set(f.get("media_types", [])),
            streams      = set(f.get("streams",     [])),
            hide_donghua = f.get("hide_donghua", False),
        )

    def to_dict(self) -> dict:
        return {
            "air_types":    list(self.air_types),
            "media_types":  list(self.media_types),
            "streams":      list(self.streams),
            "hide_donghua": self.hide_donghua,
        }

    def allows_air_type(self, air_type: str) -> bool:
        return air_type.lower() in self.air_types

    # Backward-compat alias used by _send_reminder
    def allows(self, air_type: str) -> bool:
        return self.allows_air_type(air_type)

    def as_api_params(self) -> dict:
        """
        Return extra query params to pass to /api/v3/timetables so the API
        itself filters by media type, stream, and donghua at source.
        Air-type filtering is still done post-fetch in process().
        """
        params: dict = {}
        for mt in self.media_types:
            params.setdefault("media-types", []).append(mt)
        for st in self.streams:
            params.setdefault("streams", []).append(st)
        # donghua: animeschedule uses a "no-donghua" style filter param
        # The API accepts genres-exclude but donghua is a boolean on entries
        # We handle it post-fetch via _filter_donghua flag on the entry
        return params

    def is_default(self) -> bool:
        return (
            self.air_types == {"raw", "sub", "dub"}
            and not self.media_types
            and not self.streams
            and not self.hide_donghua
        )
