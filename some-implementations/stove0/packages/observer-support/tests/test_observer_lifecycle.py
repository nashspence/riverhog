from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import httpx
import pytest
from http_api_contracts.control import ControlBudgetExhausted
from riverhog_canonical_json import canonical_json_bytes
from stove0_extension_support import ExecutionOwner
from stove0_observer_client import ContentObserverClient
from stove0_observer_protocol import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    AcceptedObservationJob,
    CollectionRootIdentityRef,
    ContentObservationInvocation,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    JsonSchemaValidationProfile,
    ObservationJobStatus,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    ObserverRuntimeAuthority,
    WorkArtifactSubject,
)
from stove0_observer_protocol.interfaces import seal_owned_interface
from stove0_observer_support import (
    ContentObservationResultBuilder,
    ContentObservationRuntime,
    ObserverHttpBinding,
    PersistentObserverService,
)
from stove0_protocol import ArtifactSelection
from stove0_protocol.observation_evidence import ObservationQuestion, ObservationQuestionPayload
from stove0_protocol.observation_interfaces import ObservationInterfaceVector


def _subject():
    return WorkArtifactSubject(
        id="fixture-member",
        role="stove0.source/v1",
        artifact_id="b" * 64,
        bytes="3",
        sha256="c" * 64,
        collection=CollectionRootIdentityRef(
            collection_id="1", archive_root_sha256="d" * 64, artifact_set_identity="e" * 64
        ),
    )


class Observer:
    def __init__(self, execute=None):
        self.contract = ObserverContract.seal(
            ObserverContractPayload(
                id="fixture.count/v1",
                facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
                options_schema=JsonSchemaValidationProfile.from_schema(
                    "fixture.options/v1", {"type": "object", "additionalProperties": False}
                ),
                facts_schema=JsonSchemaValidationProfile.from_schema(
                    "fixture.facts/v1",
                    {
                        "type": "object",
                        "properties": {"count": {"type": "integer"}},
                        "required": ["count"],
                        "additionalProperties": False,
                    },
                ),
            )
        )
        self.interface, _ = seal_owned_interface(
            contract=self.contract,
            id="fixture.count-interface/v1",
            inputs={"subjects": {"kind": "subjects"}},
            views={"count": {"kind": "global-facts", "record_at": "", "record_schema_at": ""}},
            partitioning="whole-scope",
            empty_scope="inapplicable",
            vectors=[
                ObservationInterfaceVector(
                    id="count",
                    accepted=True,
                    subjects=(_subject(),),
                    options={},
                    facts={"count": 1},
                ),
                ObservationInterfaceVector(
                    id="missing", accepted=False, subjects=(_subject(),), options={}, facts={}
                ),
            ],
        )
        self._descriptor = ObserverDescriptor.seal(
            ObserverDescriptorPayload(
                implementation_id="fixture.observer/v1",
                implementation_version="fixture",
                source_revision="fixture",
                image_id="sha256:" + "f" * 64,
                contracts=(
                    ObserverContractSupport.from_contract(
                        self.contract, interfaces=(self.interface.ref,)
                    ),
                ),
            )
        )
        self.execute = execute
        self.calls = []

    def descriptor(self):
        return self._descriptor

    def observe(self, request, runtime):
        self.calls.append((request, runtime))
        if self.execute:
            return self.execute(request, runtime)
        runtime.heartbeat()
        return ContentObservationResultBuilder(self._descriptor, request).observed(
            {"count": len(request.subjects)}
        )


def invocation(
    observer,
    *,
    claim="fixture-claim",
    fence=1,
    token="fixture-capability",
    url="https://riverhog.invalid",
):
    subjects = (_subject(),)
    selection = ArtifactSelection.seal(subjects)
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id="a" * 64,
            task_id="count",
            observer_contract=observer.interface.observer_contract,
            interface=observer.interface.ref,
            scope=selection.ref(),
            subject_ports={"subjects": selection.ref()},
            read_actions=observer.contract.read_actions,
        )
    )
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            task_id=question.task_id,
            question_sha256=question.question_sha256,
            interface=question.interface,
            work_id="a" * 64,
            observer_registration_id="fixture",
            observer_descriptor_sha256=observer.descriptor().descriptor_sha256,
            observer_contract_id=observer.contract.id,
            observer_contract_sha256=observer.contract.contract_sha256,
            subjects=subjects,
        )
    )
    return ContentObservationInvocation(
        request=request,
        claim_id=claim,
        fence=fence,
        runtime=ObserverRuntimeAuthority(
            riverhog_base_url=url,
            capability_token=token,
            allow_insecure_http=url.startswith("http:"),
            declared_workspace_protection="memory-backed",
        ),
    )


@contextmanager
def runtime_factory(invocation, *, cancellation_check):
    runtime = SimpleNamespace(
        token=invocation.runtime.capability_token, heartbeat=cancellation_check
    )
    runtime.refresh_capability = lambda token: setattr(runtime, "token", token)
    yield runtime


class Permit:
    def __init__(self, owner):
        self.owner = owner
        self.activated = False
        self.released = False

    def activate(self, cancellation, *, deadline):
        assert deadline > time.monotonic()
        self.activated = True

    def release(self, *, deadline):
        assert deadline > time.monotonic()
        self.released = True


class Admission:
    def __init__(self):
        self.allowed = False
        self.probes = []
        self.permits = []
        self.withdrawn = []
        self.probed = threading.Event()

    def probe(self, owner, *, deadline):
        self.probes.append(owner)
        self.probed.set()
        if not self.allowed:
            return None
        permit = Permit(owner)
        self.permits.append(permit)
        return permit

    def withdraw(self, owner, *, deadline):
        self.withdrawn.append(owner)


@pytest.fixture
def service_factory(tmp_path, monkeypatch):
    monkeypatch.setattr(
        PersistentObserverService, "_validate_live_authority", lambda *args, **kwargs: None
    )
    services = []

    def create(observer, **kwargs):
        service = PersistentObserverService(
            observer,
            state_root=kwargs.pop("state_root", tmp_path / f"state-{len(services)}"),
            runtime_factory=runtime_factory,
            admission_probe_seconds=0.1,
            admission_retry_seconds=0.01,
            **kwargs,
        )
        services.append(service)
        return service

    yield create
    for service in reversed(services):
        service.close()


def wait_completed(service, invocation):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status = service.get_job(invocation.job_id)
        if status.state == "completed":
            return status
        time.sleep(0.01)
    raise AssertionError("observation did not complete")


def test_invocation_binds_generation_implementation_workspace_and_evidence_but_not_secret():
    observer = Observer()
    first = invocation(observer)
    refreshed = invocation(observer, token="refreshed-secret")
    assert first.job_id == refreshed.job_id
    assert first.job_id != invocation(observer, fence=2).job_id
    assert first.job_id != invocation(observer, claim="other-claim").job_id
    altered = first.model_copy(
        update={
            "runtime": first.runtime.model_copy(
                update={"declared_workspace_protection": "encrypted-at-rest"}
            )
        }
    )
    assert first.job_id != altered.job_id
    document = first.accepted().model_dump(mode="json")
    document["fence"] = 2
    with pytest.raises(ValueError, match="identity"):
        AcceptedObservationJob.model_validate(document)
    assert "fixture-capability" not in first.accepted().model_dump_json()


def test_admission_wait_holds_no_runtime_worker_or_workspace_and_polls_preserve_attempt(
    service_factory,
):
    observer, admission = Observer(), Admission()
    service = service_factory(observer, execution_admission=admission)
    jobs = [invocation(observer, claim=f"claim-{index}") for index in range(32)]
    for job in jobs:
        assert service.put_job(job).state == "queued"
    assert admission.probed.wait(5)
    assert service._dispatch.payload_count == 0 and not service._runtimes
    assert not observer.calls
    for job in jobs:
        assert service.get_job(job.job_id).attempt == service.put_job(job).attempt == 1
        assert service.cancel_job(job.accepted()).result.state == "canceled"
    admission.allowed = True
    time.sleep(0.03)
    assert not observer.calls and not admission.permits


def test_queued_restart_requires_fresh_authority_without_spending_attempt(
    tmp_path, service_factory
):
    observer, admission = Observer(), Admission()
    first = service_factory(observer, state_root=tmp_path / "state", execution_admission=admission)
    job = invocation(observer)
    first.put_job(job)
    first.close()
    admission.allowed = True
    restarted = service_factory(
        observer, state_root=tmp_path / "state", execution_admission=admission
    )
    time.sleep(0.03)
    assert restarted.get_job(job.job_id).state == "queued" and not observer.calls
    assert restarted.put_job(invocation(observer, token="fresh-after-restart")).attempt == 1
    status = wait_completed(restarted, job)
    assert status.result.state == "observed" and status.attempt == 1
    assert observer.calls[0][1].token == "fresh-after-restart"
    for path in (tmp_path / "state").glob("*.json"):
        assert (
            "fresh-after-restart" not in path.read_text()
            and "fixture-capability" not in path.read_text()
        )


def test_running_restart_is_interrupted_and_only_explicit_refresh_retries(
    tmp_path, service_factory
):
    observer, admission = Observer(), Admission()
    service = service_factory(
        observer, state_root=tmp_path / "state", execution_admission=admission
    )
    job = invocation(observer)
    queued = service.put_job(job)
    service.close()
    path = tmp_path / "state" / f"{job.job_id}.status.json"
    path.write_text(queued.model_copy(update={"state": "running"}).model_dump_json())
    admission.allowed = True
    restarted = service_factory(
        observer, state_root=tmp_path / "state", execution_admission=admission
    )
    assert restarted.get_job(job.job_id).state == "interrupted"
    assert not observer.calls
    assert restarted.put_job(job).attempt == 2
    assert wait_completed(restarted, job).result.state == "observed"


def test_unknown_cancel_survives_restart_and_fences_delayed_first_put(tmp_path, service_factory):
    observer = Observer()
    job = invocation(observer)
    service = service_factory(observer, state_root=tmp_path / "state")
    binding = ObserverHttpBinding(service)
    canceled = binding.handle(
        "POST", f"/v1/observations/{job.job_id}/cancel", job.accepted().model_dump_json().encode()
    )
    assert ObservationJobStatus.model_validate_json(canceled.body).result.state == "canceled"
    service.close()
    restarted = service_factory(observer, state_root=tmp_path / "state")
    assert restarted.put_job(job).result.state == "canceled"
    assert not observer.calls


def test_lost_cancel_ack_is_replayed_from_cancel_only_checkpoint(tmp_path, service_factory):
    observer = Observer()
    job = invocation(observer)
    state = tmp_path / "state"
    state.mkdir()
    (state / f"{job.job_id}.cancel.json").write_text(job.accepted().model_dump_json())
    restarted = service_factory(observer, state_root=state)
    assert restarted.put_job(job).result.state == "canceled"
    assert not observer.calls


def test_canceled_inflight_probe_releases_only_exact_owned_grant(service_factory):
    observer = Observer()
    entered, release = threading.Event(), threading.Event()

    class Delayed(Admission):
        def probe(self, owner, *, deadline):
            self.permit = Permit(owner)
            entered.set()
            assert release.wait(5)
            return self.permit

    admission = Delayed()
    service = service_factory(observer, execution_admission=admission)
    job = invocation(observer)
    try:
        service.put_job(job)
        assert entered.wait(5)
        assert service.cancel_job(job.accepted()).result.state == "canceled"
    finally:
        release.set()
    service.close()
    assert admission.permit.released and not admission.permit.activated and not observer.calls


def test_foreign_grant_never_released_or_used(service_factory):
    observer = Observer()

    class Foreign(Admission):
        def probe(self, owner, *, deadline):
            self.probed.set()
            return permit

    permit = Permit(ExecutionOwner("0" * 64, 1, "foreign", "foreign"))
    admission = Foreign()
    service = service_factory(observer, execution_admission=admission)
    job = invocation(observer)
    service.put_job(job)
    assert admission.probed.wait(5)
    service.cancel_job(job.accepted())
    service.close()
    assert not permit.activated and not permit.released and not observer.calls


def test_canceling_payload_waits_for_actual_consumer_stop(service_factory):
    entered, stopped = threading.Event(), threading.Event()
    observer = Observer()

    def execute(request, runtime):
        entered.set()
        assert stopped.wait(5)
        runtime.heartbeat()
        raise AssertionError("canceled runtime must stop")

    observer.execute = execute
    service = service_factory(observer)
    job = invocation(observer)
    service.put_job(job)
    try:
        assert entered.wait(5)
        assert service.cancel_job(job.accepted()).state == "canceling"
        assert service.get_job(job.job_id).result is None
    finally:
        stopped.set()
    assert wait_completed(service, job).result.state == "canceled"


def test_valid_complete_evidence_wins_late_cancel_and_cleanup_failure(tmp_path, monkeypatch):
    completed, release = threading.Event(), threading.Event()
    observer = Observer()

    @contextmanager
    def cleanup_runtime(job, *, cancellation_check):
        yield SimpleNamespace(heartbeat=cancellation_check)
        completed.set()
        assert release.wait(5)
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(
        PersistentObserverService, "_validate_live_authority", lambda *args, **kwargs: None
    )
    service = PersistentObserverService(
        observer, state_root=tmp_path / "state", runtime_factory=cleanup_runtime
    )
    job = invocation(observer)
    try:
        service.put_job(job)
        assert completed.wait(5)
        status = service.get_job(job.job_id)
        assert status.result.state == "observed"
        assert service.cancel_job(job.accepted()) == status
    finally:
        release.set()
        service.close()
    assert service.get_job(job.job_id) == status


def test_queue_budget_is_backpressure_before_acceptance_and_does_not_reject_refresh(
    service_factory,
):
    observer, admission = Observer(), Admission()
    service = service_factory(observer, maximum_pending_jobs=1, execution_admission=admission)
    first, second = invocation(observer), invocation(observer, claim="second")
    service.put_job(first)
    binding = ObserverHttpBinding(service)
    response = binding.handle(
        "PUT", f"/v1/observations/{second.job_id}", second.model_dump_json().encode()
    )
    assert response.status == 503
    assert not (service.state_root / f"{second.job_id}.accepted.json").exists()
    assert service.put_job(invocation(observer, token="refresh")).attempt == 1
    service.cancel_job(first.accepted())
    assert service.put_job(second).state == "queued"


def test_fresh_riverhog_capability_is_checked_before_resource_probe_and_payload(tmp_path):
    authorized = False
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            requests.append(self.headers.get("Authorization"))
            if not authorized:
                body = b'{"error":{"code":"unauthorized","message":"fixture revoked"}}'
                self.send_response(401)
            else:
                body = json.dumps(
                    {"id": "1", "archive_root_sha256": "d" * 64, "artifact_set_identity": "e" * 64}
                ).encode()
                self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    observer, admission = Observer(), Admission()
    admission.allowed = True
    service = PersistentObserverService(
        observer,
        state_root=tmp_path / "state",
        runtime_factory=runtime_factory,
        execution_admission=admission,
        admission_retry_seconds=0.01,
        admission_probe_seconds=1,
    )
    job = invocation(observer, url=f"http://127.0.0.1:{server.server_port}")
    try:
        service.put_job(job)
        deadline = time.monotonic() + 5
        while not requests and time.monotonic() < deadline:
            time.sleep(0.01)
        assert requests and not admission.probes and not observer.calls
        authorized = True
        assert wait_completed(service, job).result.state == "observed"
        assert len(requests) >= 3 and admission.permits[0].activated
    finally:
        service.close()
        server.shutdown()
        server.server_close()
        thread.join()


def test_native_staged_whole_question_and_complete_testimony_preserve_identity(
    service_factory, monkeypatch
):
    observer = Observer()
    service = service_factory(observer)
    binding = ObserverHttpBinding(service)
    original = invocation(observer)
    template = _subject()
    subjects = tuple(
        template.model_copy(
            update={
                "id": f"subject-{index:05d}",
                "artifact_id": f"{index:064x}",
            }
        )
        for index in range(16384)
    )
    selection = ArtifactSelection.seal(subjects)
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id=original.request.work_id,
            task_id=original.request.task_id,
            observer_contract=observer.interface.observer_contract,
            interface=observer.interface.ref,
            scope=selection.ref(),
            subject_ports={"subjects": selection.ref()},
            read_actions=observer.contract.read_actions,
        )
    )
    payload = original.request.model_dump(mode="json", exclude={"request_id"})
    payload.update(
        subjects=[item.model_dump(mode="json") for item in subjects],
        question_sha256=question.question_sha256,
    )
    job = original.model_copy(
        update={
            "request": ContentObservationRequest.seal(
                ContentObservationRequestPayload.model_validate(payload)
            )
        }
    )
    raw = canonical_json_bytes(job.model_dump(mode="json"))
    assert len(raw) > binding.maximum_request_bytes
    assert binding.handle("PUT", f"/v1/observations/{job.job_id}", raw).status == 413
    real_client = httpx.Client
    requests = []

    def respond(request):
        assert request.headers["Authorization"] == "Bearer test-observer-token"
        requests.append((request.method, request.url.path, len(request.content)))
        response = binding.handle(request.method, request.url.path, request.content)
        return httpx.Response(
            response.status, content=response.body, headers=dict(response.headers)
        )

    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: real_client(transport=httpx.MockTransport(respond))
    )
    client = ContentObserverClient(
        "https://observer.invalid",
        token="test-observer-token",
        staged_metadata=True,
    )
    try:
        descriptor = client.descriptor()
        deadline = time.monotonic() + 90
        while True:
            assert time.monotonic() < deadline
            try:
                receipt = client.put_job(job, descriptor=descriptor)
                break
            except ControlBudgetExhausted:
                time.sleep(0.01)
        assert receipt.job_id == job.job_id and receipt.request_id == job.request.request_id
        wait_completed(service, job)
        while True:
            assert time.monotonic() < deadline
            try:
                status = client.status(job.accepted(), descriptor=descriptor)
                break
            except ControlBudgetExhausted:
                time.sleep(0.01)
        assert status.result is not None and status.result.subjects == subjects
        assert status.result.facts == {"count": len(subjects)}
        assert status.result.request_id == job.request.request_id
        assert len(observer.calls) == 1
        assert all(size < 100000 for _method, _path, size in requests)
        assert all(path.startswith("/v1/metadata/") for _method, path, _size in requests)
        assert all(
            b"fixture-capability" not in path.read_bytes()
            for path in service.state_root.rglob("*")
            if path.is_file()
        )
    finally:
        client.close()


def test_control_refresh_leaves_execution_timeout_and_cleanup_with_the_worker(
    tmp_path, monkeypatch
):
    entered, release, expired = threading.Event(), threading.Event(), threading.Event()

    def execute(request, runtime):
        entered.set()
        assert release.wait(5)
        runtime.heartbeat()
        raise AssertionError("expired execution must not produce facts")

    def real_runtime(job, *, cancellation_check):
        def check():
            if expired.is_set():
                raise TimeoutError("observer execution exceeded its deadline")
            cancellation_check()

        return ContentObservationRuntime.from_invocation(job, cancellation_check=check)

    monkeypatch.setattr(
        PersistentObserverService, "_validate_live_authority", lambda *args, **kwargs: None
    )
    observer = Observer(execute)
    service = PersistentObserverService(
        observer,
        state_root=tmp_path / "state",
        runtime_factory=real_runtime,
        admission_probe_seconds=0.1,
        admission_retry_seconds=0.01,
    )
    binding = ObserverHttpBinding(service)
    job = invocation(observer)
    path = "/v1/observations/" + job.job_id
    try:
        first = binding.handle("PUT", path, canonical_json_bytes(job.model_dump(mode="json")))
        assert first.status == 200
        assert entered.wait(2)
        expired.set()
        refreshed = invocation(observer, token="refreshed-capability")
        response = binding.handle(
            "PUT", path, canonical_json_bytes(refreshed.model_dump(mode="json"))
        )
        assert response.status == 200, response.body
        status = ObservationJobStatus.model_validate_json(response.body)
        assert status.state == "running" and status.result is None
        assert len(observer.calls) == 1
        assert observer.calls[0][1].api.current.token == "refreshed-capability"
        release.set()
        completed = wait_completed(service, job)
        assert completed.result.state == "failed"
        assert completed.result.failure.code == "observer-execution"
        assert completed.result.failure.retryable is True
        replay = binding.handle("PUT", path, canonical_json_bytes(job.model_dump(mode="json")))
        assert replay.status == 200
        assert ObservationJobStatus.model_validate_json(replay.body) == completed
        assert len(observer.calls) == 1
    finally:
        release.set()
        service.close()


def test_live_observer_retention_removes_expired_completed_cancellation_only(
    tmp_path,
    service_factory,
):
    observer, admission = Observer(), Admission()
    service = service_factory(
        observer,
        execution_admission=admission,
        terminal_state_retention_seconds=1,
    )
    canceled = invocation(observer)
    pending = invocation(observer, claim="fixture-still-pending")
    service.put_job(canceled)
    service.put_job(pending)
    assert admission.probed.wait(5)
    assert service.cancel_job(canceled.accepted()).state == "completed"
    # The terminal result remains idempotent throughout its configured window.
    assert service.put_job(canceled).state == "completed"
    import os

    for item in (canceled, pending):
        path = service.state_root / f"{item.job_id}.status.json"
        os.utime(path, (time.time() - 10, time.time() - 10))
    service._retention.wake()
    deadline = time.monotonic() + 5
    while (service.state_root / f"{canceled.job_id}.status.json").exists():
        assert time.monotonic() < deadline
        threading.Event().wait(0.01)
    assert not tuple(service.state_root.glob(f"{canceled.job_id}.*.json"))
    assert service.get_job(pending.job_id).state == "queued"
    assert service._dispatch.payload_count == 0
    assert not observer.calls


def test_observer_retention_configuration_is_positive_and_shared(monkeypatch):
    from stove0_observer_support.configuration import (
        DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
        OBSERVER_TERMINAL_STATE_RETENTION_ENV,
        terminal_state_retention_seconds,
    )

    assert terminal_state_retention_seconds({}) == DEFAULT_TERMINAL_STATE_RETENTION_SECONDS
    assert terminal_state_retention_seconds({OBSERVER_TERMINAL_STATE_RETENTION_ENV: "120"}) == 120
    for value in ("0", "-1", "unknown"):
        with pytest.raises(ValueError):
            terminal_state_retention_seconds({OBSERVER_TERMINAL_STATE_RETENTION_ENV: value})
    monkeypatch.setenv(OBSERVER_TERMINAL_STATE_RETENTION_ENV, "120")
    assert terminal_state_retention_seconds() == 120
