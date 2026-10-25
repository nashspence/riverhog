from __future__ import annotations

import hashlib
from contextlib import ExitStack
from dataclasses import asdict, replace
from functools import partial
from types import SimpleNamespace
from uuid import UUID

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_corpus_jobs as owner
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionDeletionRecord as Deletion
from riverhog_core.catalog_models import CollectionRecord as Collection
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import delete, select, update
from sqlalchemy.orm import sessionmaker

from tests.support.catalog_work import CatalogRecorder, octets
from tests.unit.test_canonical_discovery_corpus_jobs import (  # noqa: F401
    fixed_inputs as fixed_inputs,
)
from tests.unit.test_canonical_discovery_corpus_jobs import (
    original as original,
)
from tests.unit.test_canonical_discovery_corpus_jobs import (
    qualified as qualified,
)
from tests.unit.test_canonical_discovery_index import _collection

TABLES = (Collection, Generation, State, Deletion, Job)
MODES = (
    "select_discovery_job",
    "claim_discovery_worker",
    "confirm_discovery_storage",
    "discovery_worker_current",
    "pin_discovery_corpus",
    "activate_discovery_corpus",
    "lose_discovery_corpus",
    "retire_discovery_job",
)


def snapshot(engine):
    with engine.connect() as connection:
        return {
            model.__tablename__: tuple(
                sorted(tuple(row) for row in connection.execute(select(model.__table__)).fetchall())
            )
            for model in TABLES
        }


@pytest.fixture(scope="module")
def cases(tmp_path_factory, fixed_inputs):
    admission, frozen = fixed_inputs
    root = tmp_path_factory.mktemp("discovery-job-growth")
    result = []
    with ExitStack() as resources:
        for retained in (1, 17, 1025):
            for metadata_width in (1, 17, 1025):
                here = root / f"rows-{retained}-width-{metadata_width}"
                here.mkdir()
                engine = create_catalog_engine("sqlite:///" + str(here / "catalog.sqlite3"))
                resources.callback(engine.dispose)
                Base.metadata.create_all(engine)
                factory = sessionmaker(engine, expire_on_commit=False)
                with factory() as session, session.begin():
                    collection = _collection()
                    collection.archive_generation = admission.root.archive_generation
                    collection.archive_root_sha256 = admission.archive_sha256
                    collection.provenance_identity = admission.root.identity
                    collection.artifact_set_identity = admission.root.artifact_set_sha256
                    session.add(collection)
                    session.flush()
                    from riverhog_core.canonical_discovery_index import begin_index_build

                    build_id = begin_index_build(session, collection_id=1)
                catalog = ContractCatalog()
                selected = owner.select_discovery_job(
                    factory, build_id=build_id, admission=admission, catalog=catalog
                )
                assert selected is not None
                with factory() as session, session.begin():
                    build = session.get(Generation, build_id)
                    original_build = {
                        column.key: getattr(build, column.key)
                        for column in Generation.__table__.columns
                    }
                    controls = []
                    for index in range(retained):
                        other_build = str(UUID(int=index + 1))
                        session.add(Generation(**dict(original_build, build_id=other_build)))
                        values = dict(
                            asdict(selected),
                            workspace_id=f"{index + 1:032x}",
                            attempt_id=f"{index + 10001:032x}",
                            build_id=other_build,
                            state="lost",
                            failure="x" * metadata_width,
                        )
                        controls.append(Job(**values))
                    session.flush()
                    session.add_all(controls)
                lease_root = here / "owned"
                with lease_journal_validation_workspace(
                    lease_root,
                    selected.workspace_id,
                    create=True,
                    creation_current=partial(owner.discovery_worker_current, factory, selected),
                ) as path:
                    assert path is not None
                result.append(
                    SimpleNamespace(
                        engine=engine,
                        factory=factory,
                        job=selected,
                        frozen=frozen,
                        admission=admission,
                        catalog=catalog,
                        build_id=build_id,
                        root=lease_root,
                        retained=retained,
                        metadata_width=metadata_width,
                    )
                )
        yield result


def reset(case, mode, variant):
    job = replace(case.job, worker_epoch=1, state="seeding")
    if mode == "select_discovery_job":
        job = case.job
    if mode == "confirm_discovery_storage" and variant != "retry":
        job = replace(job, state="preparing")
    if mode == "activate_discovery_corpus" or (
        mode == "pin_discovery_corpus" and variant in {"retry", "post-open-retry"}
    ):
        job = replace(
            job,
            state="ready",
            source_manifest_sha256=case.frozen.sources.manifest_sha256,
            source_count=str(case.frozen.sources.source_count),
            selection_manifest_sha256=case.frozen.selection_sha256,
            selection_count=str(case.frozen.selection_count),
        )
    with case.factory() as session, session.begin():
        session.execute(delete(Job).where(Job.build_id == case.build_id))
        # A retirement from the preceding sample has no live build link.
        session.execute(delete(Job).where(Job.workspace_id == case.job.workspace_id))
        session.execute(
            update(Collection)
            .where(Collection.id == 1)
            .values(archive_root_sha256=case.admission.archive_sha256)
        )
        session.execute(update(State).where(State.collection_id == 1).values(epoch=1))
        if not (mode == "select_discovery_job" and variant == "new"):
            session.add(Job(**asdict(job)))
        if variant == "superseded":
            session.execute(update(State).where(State.collection_id == 1).values(epoch=2))
    return job


def measure(case, mode, variant, cold):
    job = reset(case, mode, variant)
    if variant == "post-open-retry":
        assert owner.activate_discovery_corpus(case.factory, job) is not None
    control = "failure" * case.metadata_width
    before = snapshot(case.engine)
    width = max(
        octets(tuple(asdict(job).values())),
        octets(control),
        *(octets(row) for rows in before.values() for row in rows),
    )
    dimensions = {
        "retained_rows": sum(map(len, before.values())),
        "metadata_row_octets": width,
        "control_octets": octets((tuple(asdict(job).values()), control)),
    }
    expected_error = None
    changed = int(mode not in {"discovery_worker_current", "select_discovery_job"})
    if mode == "select_discovery_job":
        action = partial(
            owner.select_discovery_job,
            case.factory,
            build_id=case.build_id,
            admission=case.admission,
            catalog=case.catalog,
        )
        changed = int(variant == "new")
    elif mode == "claim_discovery_worker":
        selected = replace(job, version="e" * 64) if variant == "stale" else job
        action = partial(owner.claim_discovery_worker, case.factory, selected)
    elif mode == "confirm_discovery_storage":
        selected = replace(job, version="e" * 64) if variant == "stale" else job
        action = partial(owner.confirm_discovery_storage, case.factory, selected)
        changed = int(variant != "retry")
    elif mode == "discovery_worker_current":
        action = partial(owner.discovery_worker_current, case.factory, job)
    elif mode == "pin_discovery_corpus":
        manifest = case.frozen
        if variant == "wrong-count":
            manifest = replace(manifest, sources=replace(manifest.sources, source_count=2))
            expected_error = owner.StaleArchiveTask
        action = partial(owner.pin_discovery_corpus, case.factory, job, manifest)
        changed = int(variant not in {"retry", "post-open-retry"})
    elif mode == "activate_discovery_corpus":
        selected = replace(job, version="e" * 64) if variant == "stale" else job
        action = partial(owner.activate_discovery_corpus, case.factory, selected)
    elif mode == "lose_discovery_corpus":
        selected = replace(job, version="e" * 64) if variant == "stale" else job
        action = partial(owner.lose_discovery_corpus, case.factory, selected, failure=control)
    else:
        selected = replace(job, version="e" * 64) if variant == "stale" else job
        action = partial(owner.retire_discovery_job, case.factory, selected)
    if variant in {"superseded", "stale", "wrong-count"}:
        changed = 0
    recorder = CatalogRecorder(owner)
    with ExitStack() as lease:
        if mode not in {"select_discovery_job", "discovery_worker_current"}:
            assert (
                lease.enter_context(lease_journal_validation_workspace(case.root, job.workspace_id))
                is not None
            )
        with recorder.observe(case.engine, cold=cold):
            if expected_error:
                with pytest.raises(expected_error):
                    action()
                result = None
            else:
                result = action()
    after = snapshot(case.engine)
    assert recorder.metrics["changed_rows"] == changed
    for table in TABLES[:-1]:
        assert after[table.__tablename__] == before[table.__tablename__]
    if not changed:
        assert after == before
    if variant in {"superseded", "stale"}:
        assert result in (None, False)
    elif expected_error is None:
        assert result is not None and result is not False
    if isinstance(result, owner.DiscoveryJob):
        with case.factory() as session:
            assert asdict(result) == asdict(
                owner.DiscoveryJob.read(session.get(Job, result.workspace_id))
            )
    binding = growth_binding(getattr(owner, mode))
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {
            bound.metric: recorder.metrics[bound.metric] for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


def exercise(mode, cases, record_property):
    variants = {
        "select_discovery_job": ("new", "existing", "superseded"),
        "claim_discovery_worker": ("fresh", "stale", "superseded"),
        "confirm_discovery_storage": ("fresh", "retry", "stale", "superseded"),
        "discovery_worker_current": ("current", "superseded"),
        "pin_discovery_corpus": ("fresh", "retry", "post-open-retry", "wrong-count", "superseded"),
        "activate_discovery_corpus": ("fresh", "stale", "superseded"),
        "lose_discovery_corpus": ("fresh", "stale"),
        "retire_discovery_job": ("fresh", "stale"),
    }[mode]
    samples = []
    for case in cases:
        for cold in (False, True):
            for variant in variants:
                samples.append(measure(case, mode, variant, cold))
    record_property("growth_samples", samples)
    record_property(
        "catalog_conditions",
        "Actual private catalog statements and complete native row/parameter widths; "
        "cold/warm connections; independent retained-row and metadata widths 1/17/1025; "
        "original frozen-input fixture and source-only unrelated retained metadata controls. "
        "This is job-catalog evidence, not Native lifecycle, member-State or pu"
        "blication acceptance.",
    )


@pytest.mark.growth_witness(
    "canonical-discovery-job-select/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.select_discovery_job",
)
def test_measured_discovery_job_select(cases, record_property):
    exercise("select_discovery_job", cases, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-job-claim/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.claim_discovery_worker",
)
def test_measured_discovery_job_claim(cases, record_property):
    exercise("claim_discovery_worker", cases, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-job-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.discovery_worker_current",
)
def test_measured_discovery_job_current(cases, record_property):
    exercise("discovery_worker_current", cases, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-job-pin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.pin_discovery_corpus",
)
def test_measured_discovery_job_pin(cases, record_property):
    exercise("pin_discovery_corpus", cases, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-job-activate/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.activate_discovery_corpus",
)
def test_measured_discovery_job_activate(cases, record_property):
    exercise("activate_discovery_corpus", cases, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-job-lose/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.lose_discovery_corpus",
)
def test_measured_discovery_job_lose(cases, record_property):
    exercise("lose_discovery_corpus", cases, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-job-retire/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.retire_discovery_job",
)
def test_measured_discovery_job_retire(cases, record_property):
    exercise("retire_discovery_job", cases, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-job-confirm-storage/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_jobs.confirm_discovery_storage",
)
def test_measured_discovery_job_confirm_storage(cases, record_property):
    exercise("confirm_discovery_storage", cases, record_property)
