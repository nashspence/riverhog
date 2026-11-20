from __future__ import annotations

from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload


def _different_claim(claim):
    return ("0" if claim.claim_id[0] != "0" else "1") + claim.claim_id[1:]


def _replace_claim(case, claim_id):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        upload.archive_claim_id = claim_id
