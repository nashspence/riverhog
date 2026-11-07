from __future__ import annotations

import shutil
from dataclasses import replace

import pytest
from riverhog_client.initial_tags import prepare_initial_collection_tags
from riverhog_client.producer import CollectionProducer, IncrementalCollectionProducer
from test_derived_history_admission import append, configure_disposition, execution
from test_derived_history_admission import source as source
from test_initial_tags_admission import Factory
from test_transform import (
    UploadApi,
    _completion_records,
    _derived_stream,
    inventory_from_rows,
)

from tests.support.sqlite_admission import Capacity

KINDS = (
    "regular",
    "incremental",
    "derived-incremental",
    "runtime-incremental",
    "derived-publish",
    "runtime-publish",
)


class TagApi(UploadApi):
    def __init__(self, factory):
        super().__init__()
        self.factory = factory
        self.tag_intents = []
        self.added_tags = []

    def create_or_resume_collection_upload_session(self, *args, **options):
        assert self.factory.population.active == 1
        self.tag_intents.append((tuple(options["tags"]), options["initial_tag_set_identity"]))
        return super().create_or_resume_collection_upload_session(*args, **options)

    def add_collection_upload_session_tags(self, collection, tags):
        assert self.factory.population.active == 1
        self.added_tags.append((collection, tuple(tags)))


class Ports:
    def __init__(self, factory):
        self.factory = factory

    def ports(self):
        return {
            "initial_tags_connection_factory": self.factory,
            "on_initial_tags_storage_open": self.factory.retain,
        }


def perform(kind, api, source, primary_store, factory):
    output = _derived_stream(0, b"derived payload")
    if kind in {"regular", "incremental"}:
        options = dict(
            primary_store=primary_store,
            member_inventory=lambda: inventory_from_rows(api.registered),
            producer_app="fixture-transform",
            adapter_id="fixture/v1",
            adapter_version="1",
            ingest_source="processing:fixture",
            **Ports(factory).ports(),
        )
        if kind == "regular":
            CollectionProducer(api, **options).publish_inputs(
                (replace(output, output_id=None),), source_event_id="event", poll_seconds=0.01
            )
        else:
            producer = IncrementalCollectionProducer(api, source_event_id="event", **options)
            producer.stop()
        return
    publish = kind.endswith("publish")
    with execution(
        api,
        source,
        primary_store,
        Ports(factory),
        runtime=kind.startswith("runtime"),
        publish=publish,
    ) as writer:
        if not publish:
            append(writer, source)
            return
        configure_disposition(api, source, output)
        from riverhog_archive_contracts import BOUND_HISTORY_EXTENT
        from riverhog_client.producer import ProducerArtifactIdentity

        records = _completion_records()
        writer.publish(
            (output,),
            identities={
                output.artifact_id: ProducerArtifactIdentity(
                    output.artifact_id, output.bytes, output.sha256
                )
            },
            source_histories={output.artifact_id: (source.accepted(),)},
            history_extent=BOUND_HISTORY_EXTENT,
            completion_records=records,
            execution_envelope_sha256="c" * 64,
            execution_sha256=records[2].sha256,
            disposition_set=api.derivation_identity,
            poll_seconds=0.01,
        )


@pytest.mark.parametrize("kind", KINDS)
def test_initial_tag_admission_reaches_each_actual_producer_session(source, primary_store, kind):
    factory = Factory()
    api = TagApi(factory)
    with prepare_initial_collection_tags(()) as prepared:
        expected = prepared.tag_set_identity
    perform(kind, api, source, primary_store, factory)
    assert len(factory.population.connections) == len(factory.handles) == 1
    assert api.tag_intents == [((), expected)]
    assert api.session_calls == 1 and primary_store.session_bound
    factory.assert_closed()


@pytest.mark.parametrize("kind", KINDS)
def test_initial_tag_refusal_prevents_remote_creation_and_preserves_retry_identity(
    source, primary_store, kind
):
    factory = Factory()
    api = TagApi(factory)
    factory.population.refuse = True
    with pytest.raises(Capacity):
        perform(kind, api, source, primary_store, factory)
    assert api.session_calls == 0 and not api.tag_intents
    assert not primary_store.session_bound and not factory.handles
    shutil.rmtree(factory.population.attempts[0][0].parent)
    factory.population.refuse = False
    perform(kind, api, source, primary_store, factory)
    assert api.session_calls == 1 and len(api.tag_intents) == 1
    factory.assert_closed()


@pytest.mark.parametrize("kind", ["incremental", "derived-incremental"])
def test_initial_tag_unreturned_connection_stays_with_factory_before_remote_creation(
    source, primary_store, kind
):
    factory = Factory()
    api = TagApi(factory)
    factory.population.unreturned = True
    with pytest.raises(OSError, match="construction unconfirmed"):
        perform(kind, api, source, primary_store, factory)
    assert factory.population.active == 1 and not factory.handles
    assert not primary_store.session_bound and not api.tag_intents
    factory.population.connections[0].close()
    shutil.rmtree(factory.population.attempts[0][0].parent)
    factory.population.unreturned = False
    perform(kind, api, source, primary_store, factory)
    assert api.session_calls == 1
    factory.assert_closed()


@pytest.mark.parametrize("kind", ["incremental", "derived-incremental"])
@pytest.mark.parametrize("after", [False, True])
def test_initial_tag_unknown_close_preserves_confirmed_creation_before_explicit_retry(
    source, primary_store, kind, after
):
    factory = Factory()
    api = TagApi(factory)
    retain = factory.retain

    def interrupt(prepared):
        retain(prepared)
        prepared._connection.close_failures = 1
        prepared._connection.close_after = after

    factory.retain = interrupt
    with pytest.raises(OSError, match="close unconfirmed"):
        perform(kind, api, source, primary_store, factory)
    assert primary_store.session_bound and api.session_calls == 1
    assert not api.registered
    accepted = list(api.tag_intents)
    db = factory.population.connections[0]
    assert factory.population.active == 1 and db.close_calls == 1
    factory.handles[0].close()
    assert db.close_calls == 2
    factory.retain = retain
    perform(kind, api, source, primary_store, factory)
    assert api.session_calls == 1 and api.tag_intents == accepted
    assert len(factory.handles) == len(factory.population.connections) == 2
    factory.assert_closed()
