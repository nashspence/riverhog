from __future__ import annotations

import importlib.util
import json
from io import BytesIO
from pathlib import Path

import pytest
from riverhog_provenance import validate_journal, validate_journal_set, verify_delivery
from riverhog_provenance_contracts import PROFILE

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"
NAMES = [
    "never-named-emission",
    "mixed-storage-history",
    "reported-prehistory-and-correction",
    "independent-journal-fork",
    "simultaneous-deliveries",
]


@pytest.mark.parametrize("name", NAMES)
def test_example_journal_and_materialization_agree(name, catalog):
    raw = (EXAMPLES / (name + ".jsonseq")).read_bytes()
    result = validate_journal(raw, catalog=catalog)
    expected = json.loads((EXAMPLES / (name + ".materialized.json")).read_text())
    assert result.materialize() == expected
    catalog.validate(PROFILE + "/materialized.schema.json", expected)


def test_examples_resolve_foreign_graph_and_fork(catalog):
    validate_journal_set(
        [(EXAMPLES / (name + ".jsonseq")).read_bytes() for name in NAMES], catalog=catalog
    )


def test_each_simultaneous_delivery_selects_its_own_evidence(catalog):
    result = validate_journal(
        (EXAMPLES / "simultaneous-deliveries.jsonseq").read_bytes(), catalog=catalog
    )
    assert len(result.delivery_associations) == 2
    for association in result.delivery_associations:
        verified = verify_delivery(
            result, association["id"], BytesIO((EXAMPLES / "primary.bin").read_bytes())
        )
        assert verified["scope"] == "primary_bytes_only"


def test_example_generation_is_reproducible(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "example_generator", ROOT / "tools/generate_examples.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.P = tmp_path
    module.main()
    for produced in tmp_path.iterdir():
        assert produced.read_bytes() == (EXAMPLES / produced.name).read_bytes(), produced.name


def test_alignment_does_not_infer_specialization_from_membership():
    import rdflib
    from rdflib.namespace import OWL, RDFS

    graph = rdflib.Graph().parse(
        ROOT.parent / "riverhog-provenance-contracts/formal/riverhog-provenance.ttl",
        format="turtle",
    )
    rhp = rdflib.Namespace(PROFILE + "/ontology#")
    prov = rdflib.Namespace("http://www.w3.org/ns/prov#")
    assert (rhp.capturedBy, RDFS.subPropertyOf, prov.wasGeneratedBy) in graph
    assert (rhp.observedState, RDFS.subPropertyOf, prov.used) in graph
    for property in (rhp.occurrenceOf, rhp.stateOf, rhp.previousEntry):
        assert (property, RDFS.subPropertyOf, prov.specializationOf) not in graph
        assert (property, RDFS.subPropertyOf, prov.wasDerivedFrom) not in graph
    assert not list(graph.triples((None, OWL.equivalentClass, None)))


def test_formal_invariant_index_names_real_tests():
    manifest = json.loads(
        (ROOT.parent / "riverhog-provenance-contracts/formal/invariants.json").read_text()
    )
    for invariant in manifest["invariants"]:
        for target in invariant["tests"]:
            package, filename, function = target.split(":")
            path = ROOT.parent / package / "tests" / filename
            assert f"def {function}(" in path.read_text(), target
