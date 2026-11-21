from __future__ import annotations

from contextlib import closing

import pytest
import test_assertion_staging as staging
import test_correction_projection as projection
import test_correction_targets as targets
import test_entry_preparation as preparation
import test_generic_semantics as generic
import test_semantic_edge as edge
import test_semantic_finish as finish


@pytest.mark.parametrize(
    "kind", ["preparation", "staging", "targets", "retirement", "generic", "finish", "edge"]
)
def test_child_application_uses_its_original_typed_before_state_without_a_codec_roundtrip(
    tmp_path, catalog, monkeypatch, kind
):
    path = tmp_path / "child.sqlite3"
    module = {
        "preparation": preparation,
        "staging": staging,
        "targets": targets,
        "retirement": projection,
        "generic": generic,
        "finish": finish,
        "edge": edge,
    }[kind]
    if kind == "generic":
        source = generic.subject(1)
        generic.create(path, source)
    else:
        source = module.create(path)
    with closing(module.connect(path)) as db:
        if kind == "preparation":
            current = preparation.work(db, preparation.frame())
        elif kind == "staging":
            current = staging.work(db, staging.frame((staging.row(0),)))
        elif kind == "targets":
            current = targets.work(db, source)
        elif kind == "generic":
            current = generic.work(db, source, catalog)
        else:
            current = module.work(db)
        before = current.initialize()
        batch = current.prepare()
        assert type(batch.before) is type(before) and batch.before == before

        def refuse_roundtrip(*_args, **_kwargs):
            pytest.fail("detached typed child control was decoded again during application")

        with monkeypatch.context() as patch:
            patch.setattr(type(before), "from_json", refuse_roundtrip)
            db.execute("BEGIN IMMEDIATE").close()
            try:
                after = current._apply(batch)
                db.commit()
            except BaseException:
                db.rollback()
                raise
        assert current.load() == after
