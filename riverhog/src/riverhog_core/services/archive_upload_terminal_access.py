"""Current read authority over an original retained upload-terminal context."""

from __future__ import annotations

import json

from riverhog_protocol.errors import NotFound
from sqlalchemy.orm import Session

from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    COLLECTIONS_CREATE,
    COLLECTIONS_DELETE,
    Principal,
)
from riverhog_core.collection_access import permission_resources, tag_hashes
from riverhog_core.services.archive_upload_drain import read_terminal_selection
from riverhog_core.services.archive_upload_terminal_tags import TerminalTagSelector
from riverhog_core.services.collection_tags import (
    collection_tag_sha256,
    read_collection_tag_set,
)
from riverhog_core.services.collection_upload_authority import (
    retain_upload_claim,
    retain_upload_key,
)


def require_terminal_creator_scope(
    session: Session, selector: TerminalTagSelector, principal: Principal
) -> None:
    """Check the selected original set without counting today's staged rows.

    A wildcard needs no tree traversal. A restricted proof consumes at most
    its explicit grant count plus one tag; a larger original set refuses
    without a whole-resource count or scan. Tag paths and individual node/tag
    bytes retain their existing protocol bounds.
    """
    resources = permission_resources(principal, COLLECTIONS_CREATE)
    if ALL_RESOURCES in resources:
        return
    allowed = tag_hashes(resources)
    if not allowed or selector.root_sha256 is None:
        raise NotFound("collection creation is not available")
    tags = read_collection_tag_set(session, root_sha256=selector.root_sha256)
    for index, tag in enumerate(tags.iter_tags()):
        if index >= len(allowed) or collection_tag_sha256(tag) not in allowed:
            raise NotFound("collection creation is not available")


def require_terminal_read_access(
    session: Session,
    collection_id: int,
    operation_id: str,
    principal: Principal,
) -> None:
    """Check current read rights inside the caller's repeatable read snapshot.

    The creator keeps its current creation scope over the original selected
    tags. Other readers need current deletion rights for this collection ID.
    Key validity and delegated claims remain live checks. Readback does not
    require orphan selection, an inactive output claim or a new disposal grant.
    """
    retain_upload_key(session, principal, for_update=False)
    retain_upload_claim(session, principal.id, principal.key_id, for_update=False)
    if not principal.allows(COLLECTIONS_CREATE) and not principal.allows_collection(
        COLLECTIONS_DELETE, collection_id
    ):
        raise NotFound("collection upload terminal not found")
    control, _, selector = read_terminal_selection(session, collection_id, operation_id)
    if json.loads(control.construction_json)["principal_id"] == principal.id:
        require_terminal_creator_scope(session, selector, principal)
        return
    if not principal.allows_collection(COLLECTIONS_DELETE, collection_id):
        raise NotFound("collection upload terminal not found")
