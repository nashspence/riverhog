from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
import riverhog_core
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.growth_bounds import bound_envelope, compact_expression
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_growth_contracts import Expression, declare_growth, evaluate_bound, growth_binding

from scripts.growth_contract import source_bindings


@pytest.mark.parametrize("value", [1, 17, 1025])
def test_compacted_envelopes_preserve_complete_actual_owner_bounds(value):
    original = growth_binding(UploadJournalValidationOwner.advance)
    arguments = {
        item.name: Expression("dimension", name=item.name)
        for item in original.declaration.dimensions
    }
    envelope = replace(
        original.declaration,
        id="fixture.compacted-owner.growth/v1",
        subjects=("fixture.compacted_owner",),
        bounds=tuple(
            replace(
                bound,
                expression=bound_envelope(original.operation, bound.metric, arguments),
            )
            for bound in original.declaration.bounds
        ),
    )

    def operation():
        pass

    declare_growth(envelope)(operation)
    current = growth_binding(operation)
    dimensions = {name: value for name in arguments}
    for bound in original.declaration.bounds:
        assert evaluate_bound(current, bound.metric, dimensions) == evaluate_bound(
            original, bound.metric, dimensions
        )


def test_compaction_preserves_typed_zero_and_unit_cancellation():
    count = (("count", 1),)
    inverse = (("count", -1),)
    octets = (("octets", 1),)
    zero = Expression("constant", value=0, unit=octets)
    assert compact_expression(Expression("sum", terms=(zero, zero))) == zero
    body = Expression("dimension", name="bytes")
    nested = Expression(
        "product",
        terms=(
            Expression("constant", value=1, unit=count),
            Expression(
                "product",
                terms=(Expression("constant", value=1, unit=inverse), body),
            ),
        ),
    )
    assert compact_expression(nested) == body
    assert compact_expression(Expression("sum", terms=(zero, body))) == body
    typed = Expression(
        "product",
        terms=(Expression("constant", value=1, unit=count), body),
    )
    compacted = compact_expression(typed)
    assert compacted != body and set(compacted.terms) == set(typed.terms)


def test_archive_source_declarations_enter_repository_contract_discovery():
    bindings = source_bindings({}, packages={"riverhog_core": Path(riverhog_core.__file__)})
    operations = {binding.operation for binding in bindings}
    assert {
        ArchiveJournalSource.__init__,
        ArchiveJournalSource.verify_current,
        ArchiveJournalSource.read_window,
        ArchiveJournalSource.read_exact,
    } <= operations
