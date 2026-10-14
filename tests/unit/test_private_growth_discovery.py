from __future__ import annotations

import importlib
import sys

import pytest

from scripts.growth_contract import declaration_projection, source_analysis, source_bindings


@pytest.fixture
def private_owner(tmp_path, monkeypatch):
    package = tmp_path / "private_growth_owner"
    package.mkdir()
    initializer = package / "__init__.py"
    initializer.write_text('PUBLIC_VALUE = 1\n__all__ = ["PUBLIC_VALUE"]\n')
    implementation = package / "_implementation.py"
    implementation.write_text("""from riverhog_protocol.growth import (
    COUNT, Bound, Declaration, Dimension, Expression, declare_growth,
)

def contract(identity, subject):
    return Declaration(
        id=identity, authority="fixture", subjects=(subject,),
        scope="successful-call", starts_at="accept", ends_at="return",
        dimensions=(Dimension("items", COUNT, "Admitted items."),),
        bounds=(Bound("items", COUNT, "equal",
                      Expression("dimension", name="items"), "Actual items."),),
        conditions=("Valid finite input.",), invariants=("Exact return.",),
        limitations=("Caller costs are separate.",),
    )

MODULE = "private_growth_owner._implementation"

@declare_growth(contract("fixture.private-read.growth/v1", MODULE + "._read"))
def _read(items):
    return items

alias = _read

class _Reader:
    @declare_growth(contract("fixture.private-method.growth/v1", MODULE + "._Reader._read"))
    def _read(self, items):
        return items

    @classmethod
    @declare_growth(contract("fixture.private-classmethod.growth/v1",
                            MODULE + "._Reader._class_read"))
    def _class_read(cls, items):
        return items

    @staticmethod
    @declare_growth(contract("fixture.private-staticmethod.growth/v1",
                            MODULE + "._Reader._static_read"))
    def _static_read(items):
        return items

    @property
    @declare_growth(contract("fixture.private-property.growth/v1", MODULE + "._Reader._property"))
    def _property(self):
        return 1
""")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    try:
        yield tmp_path, initializer, implementation
    finally:
        for name in tuple(sys.modules):
            if name == "private_growth_owner" or name.startswith("private_growth_owner."):
                del sys.modules[name]


def test_private_source_declarations_share_projection_without_public_exports(private_owner):
    root, initializer, implementation = private_owner
    packages = {"private_growth_owner": initializer}
    binding = source_bindings({}, packages=packages)
    assert len(binding) == 5
    assert len({id(row.operation) for row in binding}) == 5
    projection = declaration_projection({}, packages=packages)
    audit = source_analysis(root, {}, projection, packages=packages)
    assert len(projection["declarations"]) == 5 and audit["coverage"] == {}
    assert {row["source"]["path"] for row in audit["declarations"].values()} == {
        implementation.relative_to(root).as_posix(),
    }
    assert set(audit["declarations"]) == set(projection["declarations"])
    assert audit["evidence_status"] == "not-executed"


def test_public_alias_and_private_discovery_retain_one_actual_owner(private_owner):
    _, initializer, _ = private_owner
    surface = {
        "private_growth_owner._implementation.alias": {"contract": {"kind": "function"}},
    }
    assert len(source_bindings(surface, packages={"private_growth_owner": initializer})) == 5


def test_private_discovery_rejects_foreign_loaded_module(private_owner, monkeypatch):
    _, initializer, implementation = private_owner
    module = importlib.import_module("private_growth_owner._implementation")
    monkeypatch.setattr(module, "__file__", str(implementation.with_name("foreign.py")))
    with pytest.raises(ValueError, match="source authority"):
        source_bindings({}, packages={"private_growth_owner": initializer})
