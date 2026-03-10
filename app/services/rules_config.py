"""Rules/config for Timesheets Flagger.

Keep these as plain Python constants so they can be edited without touching SQL.
"""

from __future__ import annotations

# Fixed fee client list (case-insensitive exact match on Client)
FIXED_FEE_CLIENTS = {
    "Tinova Resources Corp.",
    "Town of high level",
    "1497290 B.C Ltd (Formerly Bahia Metals Corp.)",
    "Digital Dental",
    "Ciscom Corp.",
    "Peruvian Metals Corp.",
    "BON Intelligence",
    "Bulgold",
    "Alzai Heath Corp",
    "Vector Science and Therapeutics Inc.",
}

# Fee type labels as they appear in the dataset (used for comparison and messaging).
FEE_TYPE_FIXED = "Fixed Fee"
FEE_TYPE_HOURLY = "Hourly"

# TW (Treewalk/internal/admin) task-type keywords.
# We treat a row as "TW-coded" if Task Type contains any of these.
TW_TASK_KEYWORDS = [
    "administration",
    "treewalk internal",
    "treewalk billing",
    "treewalk",
]

# PTO/holiday keywords. A row is treated as PTO if Task Type / Work / Notes contains any of these.
PTO_KEYWORDS = [
    "pto",
    "vacation",
    "stat holiday",
    "holiday",
    "time off",
]

# Treewalk internal client keywords. We treat a row as "Treewalk client" if Client contains any of these.
TREEWALK_CLIENT_KEYWORDS = [
    "treewalk",
]

# BC statutory holidays (observed) for 2025 and 2026.
# Stored as YYYY-MM-DD strings.
BC_STAT_HOLIDAYS = [
    # 2025
    "2025-01-01",  # New Year's Day
    "2025-02-17",  # Family Day
    "2025-04-18",  # Good Friday
    "2025-05-19",  # Victoria Day
    "2025-07-01",  # Canada Day
    "2025-08-04",  # B.C. Day
    "2025-09-01",  # Labour Day
    "2025-09-30",  # Truth and Reconciliation Day
    "2025-10-13",  # Thanksgiving
    "2025-11-11",  # Remembrance Day
    "2025-12-25",  # Christmas Day
    "2025-12-26",  # Boxing Day
    # 2026
    "2026-01-01",  # New Year's Day
    "2026-02-16",  # Family Day
    "2026-04-03",  # Good Friday
    "2026-05-18",  # Victoria Day
    "2026-07-01",  # Canada Day
    "2026-08-03",  # B.C. Day
    "2026-09-07",  # Labour Day
    "2026-09-30",  # Truth and Reconciliation Day
    "2026-10-12",  # Thanksgiving
    "2026-11-11",  # Remembrance Day
    "2026-12-25",  # Christmas Day
    "2026-12-26",  # Boxing Day
]
