"""Current public identity domain. This hard cut does not increment versions."""

from riverhog_provenance_contracts import ENTRY_SCHEMA
from riverhog_provenance_contracts import PROFILE as PROFILE

PACKAGE_NAME = "riverhog-provenance"
PACKAGE_VERSION = "0.1.0"
PROVENANCE_PROFILE = PROFILE
PROVENANCE_ENTRY_SCHEMA = ENTRY_SCHEMA
PRIMARY_CONTENT_CATEGORY = PROFILE + "/coverage/primary_content"
OBSERVATION_PLAN = PROFILE + "/plans/opaque-bounded-source-observation"
ASSIGNED_IDENTITY_POLICY = PROFILE + "/policies/assigned-continuity"
JOURNAL_POLICY = PROFILE + "/policies/single-writer"
PROVENANCE_JOURNAL_ENTRY_BYTES_MAX = 8 * 1024 * 1024
