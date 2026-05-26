"""
Kronos Trading System — Module 3: Macro Calendar
Sections 8.4, 10.1, 13, 18.2 of the requirements spec (v2.0).

Checks whether a major macro event falls within the next 4 hours and blocks
new entries accordingly. The calendar is manually maintained by the human
operator in macro_calendar.json — updated weekly per Section 13.

Output: MacroStatus dataclass
  is_blocked:            True if any event is within the next 4H window
  hours_until_next:      hours until the nearest upcoming event (float; inf if none)
  blocking_event_name:   name of the event causing the block (None if clear)
  next_event_name:       name of the next upcoming event regardless of block
  next_event_time:       Unix timestamp of that event (None if none scheduled)
  checked_at:            Unix timestamp of this check

Blackout rule (Section 8.4 / 10.1):
  No new entries if any event is within ±4 hours of now — 4 hours before OR
  4 hours after the event time. "Within 4 hours of" in Section 8.4 is a
  symmetric window: pre-event uncertainty AND post-event volatility are both
  protected. Section 19.2 confirms: "resumes automatically after event window
  passes" — the window ends 4H after the event, not at event time.

Calendar format (macro_calendar.json):
  JSON array of objects, each with:
    name      (str)  — human-readable event name
    timestamp (int)  — Unix epoch seconds of event time (UTC)
    type      (str)  — "fed" | "cpi" | "nfp" | "crypto" | "other"
    notes     (str, optional) — free text for human reference
"""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from typing import Optional

from db import log_event

logger = logging.getLogger(__name__)

MODULE = 'macro_calendar'

BLACKOUT_SECONDS = 4 * 3600   # 4-hour window per Section 8.4 and 10.1

CALENDAR_PATH = os.environ.get(
    'KRONOS_MACRO_CALENDAR',
    os.path.join(os.path.abspath(os.path.dirname(__file__) or '.'), 'macro_calendar.json'),
)


@dataclass
class MacroStatus:
    is_blocked:           bool           # True if any event within next 4H
    hours_until_next:     float          # hours to nearest upcoming event (inf = none)
    blocking_event_name:  Optional[str]  # name of blocking event, or None if clear
    next_event_name:      Optional[str]  # name of next upcoming event (None if none)
    next_event_time:      Optional[int]  # Unix timestamp of next upcoming event
    checked_at:           int            # Unix timestamp of this check


class MacroCalendar:
    """
    Checks whether a macro event falls within the 4-hour blackout window.

    Usage:
        calendar = MacroCalendar()
        status = calendar.check()
        if status.is_blocked:
            # do not enter new positions
    """

    def check(self) -> MacroStatus:
        """
        Load the calendar, find upcoming events, return MacroStatus.
        Logs the result to the events table.
        """
        now = int(time.time())
        events = self._load_calendar()

        # Symmetric ±4H window: blocked if |event_time - now| <= BLACKOUT_SECONDS.
        # This covers 4H before the event (pre-event uncertainty) and 4H after
        # (post-event volatility). Section 19.2: "resumes after event window passes."
        blocking_event = None
        for e in events:
            ts = e.get('timestamp', 0)
            if abs(ts - now) <= BLACKOUT_SECONDS:
                # Among all blocking events, report the one closest to now
                if blocking_event is None or abs(ts - now) < abs(blocking_event.get('timestamp', 0) - now):
                    blocking_event = e

        is_blocked          = blocking_event is not None
        blocking_event_name = blocking_event.get('name') if blocking_event else None

        # Nearest upcoming event (for informational hours_until_next)
        upcoming = [e for e in events if e.get('timestamp', 0) > now]
        upcoming.sort(key=lambda e: e['timestamp'])
        next_event      = upcoming[0] if upcoming else None
        next_event_ts   = next_event.get('timestamp') if next_event else None
        next_event_name = next_event.get('name') if next_event else None

        # Hours until nearest upcoming event
        if next_event_ts is not None:
            hours_until = (next_event_ts - now) / 3600
        else:
            hours_until = float('inf')

        status = MacroStatus(
            is_blocked=is_blocked,
            hours_until_next=round(hours_until, 2) if hours_until != float('inf') else float('inf'),
            blocking_event_name=blocking_event_name,
            next_event_name=next_event_name,
            next_event_time=next_event_ts,
            checked_at=now,
        )

        self._log_status(status)
        return status

    # ── Calendar loading ────────────────────────────────────────────────────────

    @staticmethod
    def _load_calendar() -> list:
        """Load events from macro_calendar.json. Returns empty list on any error."""
        if not os.path.exists(CALENDAR_PATH):
            log_event(MODULE, 'warning', 'warning',
                      f'macro_calendar.json not found at {CALENDAR_PATH} — '
                      f'treating as no events scheduled')
            return []
        try:
            with open(CALENDAR_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, list):
                raise ValueError('macro_calendar.json must be a JSON array')
            return data
        except Exception as e:
            log_event(MODULE, 'error', 'error',
                      f'Failed to load macro_calendar.json: {e}')
            logger.exception('Failed to load macro_calendar.json')
            return []

    # ── Logging ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _log_status(status: MacroStatus) -> None:
        if status.is_blocked:
            message = (f'BLOCKED — {status.blocking_event_name} '
                       f'in {status.hours_until_next:.1f}H')
            severity = 'warning'
        elif status.next_event_name:
            message = (f'clear — next event: {status.next_event_name} '
                       f'in {status.hours_until_next:.1f}H')
            severity = 'info'
        else:
            message = 'clear — no events scheduled'
            severity = 'info'

        log_event(MODULE, severity, 'macro_check', message, asdict(status))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    from db import init_db
    init_db()
    status = MacroCalendar().check()
    print(f'is_blocked:          {status.is_blocked}')
    print(f'hours_until_next:    {status.hours_until_next}')
    print(f'blocking_event_name: {status.blocking_event_name}')
    print(f'next_event_name:     {status.next_event_name}')
