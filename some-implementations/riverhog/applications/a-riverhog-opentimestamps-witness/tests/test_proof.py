# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest
from a_riverhog_opentimestamps_witness import proof as core
from opentimestamps.core.notary import (
    BitcoinBlockHeaderAttestation,
    PendingAttestation,
    UnknownAttestation,
)
from opentimestamps.core.op import OpAppend, OpSHA1, OpSHA256
from opentimestamps.core.serialize import BytesSerializationContext
from opentimestamps.core.timestamp import DetachedTimestampFile, Timestamp
from reference_sqlite import StaleWorker, Store, initialize

A = "https://calendar-a.example"
B = "https://calendar-b.example"
ALLOW = frozenset({A, B})
DIGEST = hashlib.sha256(b"opaque caller-owned statement; not a Riverhog contract").digest()
NONCE = bytes(range(32))
VECTOR = json.loads(Path(__file__).with_name("genesis-vector.json").read_text())
REAL_DIGEST = bytes.fromhex(VECTOR["statement_digest"])
REAL_PROOF = bytes.fromhex(VECTOR["proof"])
HEADER = bytes.fromhex(VECTOR["header"])


def raw(node):
    ctx = BytesSerializationContext()
    node.serialize(ctx)
    return ctx.getbytes()


def pending_node(commitment, url=A):
    node = Timestamp(commitment)
    node.attestations.add(PendingAttestation(url))
    return node


class Calendar:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def request(self, *args):
        self.calls.append(args)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response(*args) if callable(self.response) else self.response


class Chain:
    def __init__(self, *, height=0, header=HEADER, tip=5):
        self.height, self.header, self.tip = height, header, tip
        self.calls = []

    def anchor(self, height):
        self.calls.append(height)
        if height != self.height:
            raise core.BitcoinUnavailable()
        return core.Anchor(
            height,
            hashlib.sha256(hashlib.sha256(self.header).digest()).digest()[::-1].hex(),
            self.header,
            self.tip,
            "11" * 32,
        )


def initial(*, urls=(A,), now=100):
    return core.prepare(DIGEST, NONCE, urls, now=now)


def submit(job=None):
    if job is None:
        job = initial()
    calendar = Calendar(lambda kind, url, digest, limit: raw(pending_node(digest, url)))
    return core.step(job, calendar, now=100, allow=ALLOW)


def real_pending_job():
    proof = core.write_proof(pending_node(REAL_DIGEST))
    return core.Job(
        REAL_DIGEST, NONCE, (A,), proof, (core.Work("upgrade", A, REAL_DIGEST, due=100),)
    )


def test_newly_allowed_pending_calendar_is_discovered_without_network():
    pending_job = core.Job(
        REAL_DIGEST, NONCE, (A,), core.write_proof(pending_node(REAL_DIGEST)), ()
    )
    paused = core.enable_calendars(pending_job, allow=frozenset(), now=100)
    assert paused.work == ()
    resumed = core.enable_calendars(paused, allow=frozenset({A}), now=120)
    assert resumed.work == (core.Work("upgrade", A, REAL_DIGEST, due=120),)


def mature_response():
    node = Timestamp(REAL_DIGEST)
    node.ops.add(OpSHA256()).attestations.add(BitcoinBlockHeaderAttestation(0))
    return raw(node)


def test_independent_genesis_wire_vector_and_confirmation_policy():
    assert hashlib.sha256(bytes.fromhex(VECTOR["transaction"])).digest() == REAL_DIGEST
    result = core.verify(REAL_PROOF, REAL_DIGEST, Chain(tip=5))
    assert result.cryptographically_valid
    assert result.checks[0].block_hash == VECTOR["block_hash"]
    assert result.checks[0].block_time == 1231006505
    assert result.checks[0].confirmations == 6
    assert core.confirmation_policy(result)
    assert core.confirmation_policy(result, 6)
    assert not core.confirmation_policy(result, 7)
    assert result.checks[0].status == "valid"


def test_upstream_can_read_and_reemit_independent_vector():
    assert core.write_proof(core.read_proof(REAL_PROOF, REAL_DIGEST)) == REAL_PROOF


def test_caller_digest_is_not_hashed_a_second_time_as_file_input():
    job = submit()
    assert core.read_proof(job.proof, DIGEST).msg == DIGEST
    assert job.submission_commitment == hashlib.sha256(DIGEST + NONCE).digest()
    assert job.submission_commitment != hashlib.sha256(DIGEST).digest()


@pytest.mark.parametrize("value", [b"", b"a" * 31, b"a" * 33, "aa" * 32, bytearray(32), None])
def test_bad_caller_digest(value):
    with pytest.raises(ValueError):
        core.prepare(value, NONCE, (A,), now=0)


@pytest.mark.parametrize(
    "url",
    [
        "http://calendar.example",
        "https://u:p@calendar.example",
        "https://calendar.example/",
        "https://calendar.example?q=1",
        "https://calendar.example#x",
        "",
        "file:///x",
    ],
)
def test_bad_configured_calendar(url):
    with pytest.raises(ValueError):
        initial(urls=(url,))


def test_prepare_no_io_restart_resubmits_same_blinded_commitment():
    job = initial()
    calendar = Calendar(core.CalendarError())
    failed = core.step(job, calendar, now=100, allow=ALLOW)
    restarted = core.load_job(core.dump_job(failed), DIGEST)
    assert restarted.work[0].due == 160
    assert restarted.work[0].attempts == 1
    again = core.step(restarted, calendar, now=160, allow=ALLOW)
    assert calendar.calls[0][2] == calendar.calls[1][2] == job.submission_commitment
    assert again.work[0].due == 280
    assert again.work[0].attempts == 2


def test_no_due_work_makes_no_request():
    calendar = Calendar(b"bad")
    job = initial()
    assert core.step(job, calendar, now=99, allow=ALLOW) == job
    assert not calendar.calls


def test_one_exchange_and_other_calendars_not_starved():
    job = initial(urls=(A, B))
    calendar = Calendar(core.CalendarError())
    one = core.step(job, calendar, now=100, allow=ALLOW)
    two = core.step(one, calendar, now=100, allow=ALLOW)
    assert [call[1] for call in calendar.calls] == [A, B]
    assert all(w.attempts == 1 for w in two.work)


def test_nested_pending_uses_derived_subnode_commitment():
    job = initial()
    root = Timestamp(job.submission_commitment)
    tip = root.ops.add(OpAppend(b"calendar nonce")).ops.add(OpSHA256())
    tip.attestations.add(PendingAttestation(B))
    saved = core.step(job, Calendar(raw(root)), now=100, allow=ALLOW)
    calendar = Calendar(core.CalendarError("not_found"))
    failed = core.step(saved, calendar, now=100, allow=ALLOW)
    assert calendar.calls[0][:3] == ("upgrade", B, tip.msg)
    assert tip.msg != DIGEST and tip.msg != job.submission_commitment
    assert failed.proof == saved.proof


@pytest.mark.parametrize(
    "response",
    [b"", b"junk", b"x" * (core.MAX_PROOF + 1), mature_response() + b"trailing", REAL_PROOF],
)
def test_bad_calendar_response_retains_prior_proof_and_retries(response):
    job = real_pending_job()
    failed = core.step(job, Calendar(response), now=100, allow=ALLOW)
    assert failed.proof == job.proof
    assert failed.work[0].error == "bad_proof"
    assert failed.work[0].due == 160


def test_not_found_and_transient_outage_are_not_terminal():
    job = submit()
    for code in ("not_found", "unavailable"):
        job = core.step(job, Calendar(core.CalendarError(code)), now=job.next_due, allow=ALLOW)
        assert job.proof is not None and job.work[0].due is not None
        assert job.work[0].error == code


def test_permanent_transport_failure_disables_work_but_retains_evidence():
    job = submit()
    failure = Calendar(core.CalendarError("rejected", retryable=False))
    disabled = core.step(job, failure, now=100, allow=ALLOW)
    assert disabled.proof == job.proof and disabled.work[0].due is None
    assert core.step(disabled, failure, now=99999, allow=ALLOW) == disabled
    assert len(failure.calls) == 1


def test_untrusted_pending_uri_retained_but_never_fetched():
    job = initial()
    root = pending_node(job.submission_commitment, "http://127.0.0.1:1234")
    saved = core.step(job, Calendar(raw(root)), now=100, allow=ALLOW)
    calendar = Calendar(b"bad")
    assert not saved.work
    assert core.step(saved, calendar, now=1000, allow=ALLOW) == saved
    assert not calendar.calls
    assert ("http://127.0.0.1:1234", job.submission_commitment) in core.pending(
        core.read_proof(saved.proof, DIGEST)
    )


def test_allowlist_removal_takes_effect_after_restart():
    job = core.load_job(core.dump_job(submit()), DIGEST)
    calendar = Calendar(b"bad")
    core.step(job, calendar, now=9999, allow=frozenset())
    assert not calendar.calls


def test_real_maturation_across_database_restarts(tmp_path):
    path = tmp_path / "witness.sqlite"
    initialize(path)
    store = Store(path)
    revision, job = store.enqueue(real_pending_job())
    failed = core.step(job, Calendar(core.CalendarError("not_found")), now=100, allow=ALLOW)
    store.save(revision, failed)
    store.close()
    store = Store(path)
    revision, job = store.load(REAL_DIGEST)
    assert job.next_due == 160 and job.work[0].attempts == 1
    mature = core.step(job, Calendar(mature_response()), now=160, allow=ALLOW)
    store.save(revision, mature)
    store.close()
    store = Store(path)
    _, loaded = store.load(REAL_DIGEST)
    result = core.verify(loaded.proof, REAL_DIGEST, Chain(tip=5))
    assert result.cryptographically_valid and core.confirmation_policy(result, 6)
    assert result.pending_count == 1  # prior evidence was not pruned
    assert store.connection.execute("SELECT count(*) FROM proofs").fetchone()[0] == 2
    assert loaded.work[0].attempts == 2
    store.close()


def test_crash_after_network_before_commit_replays_without_new_nonce(tmp_path):
    path = tmp_path / "witness.sqlite"
    initialize(path)
    store = Store(path)
    _, job = store.enqueue(initial())
    calendar = Calendar(lambda kind, url, digest, limit: raw(pending_node(digest, url)))
    first = core.step(job, calendar, now=100, allow=ALLOW)
    store.close()  # deliberately do NOT save first
    store = Store(path)
    revision, restarted = store.load(DIGEST)
    second = core.step(restarted, calendar, now=100, allow=ALLOW)
    assert first == second and calendar.calls[0] == calendar.calls[1]
    store.save(revision, second)
    store.close()


def test_stale_worker_cannot_overwrite_newer_proof(tmp_path):
    path = tmp_path / "witness.sqlite"
    initialize(path)
    one, two = Store(path), Store(path)
    revision, job = one.enqueue(real_pending_job())
    other_revision, old = two.load(REAL_DIGEST)
    mature = core.step(job, Calendar(mature_response()), now=100, allow=ALLOW)
    one.save(revision, mature)
    with pytest.raises(StaleWorker):
        two.save(other_revision, old)
    assert two.load(REAL_DIGEST)[1].proof == mature.proof
    one.close()
    two.close()


def test_duplicate_enqueue_preserves_nonce_and_progress(tmp_path):
    path = tmp_path / "witness.sqlite"
    initialize(path)
    store = Store(path)
    revision, first = store.enqueue(initial())
    saved = submit(first)
    store.save(revision, saved)
    duplicate = core.prepare(DIGEST, b"z" * 32, (B,), now=9999)
    assert store.enqueue(duplicate) == (1, saved)
    store.close()


def test_transaction_failure_rolls_back_proof_and_progress(tmp_path):
    path = tmp_path / "witness.sqlite"
    initialize(path)
    store = Store(path)
    revision, job = store.enqueue(real_pending_job())
    store.connection.execute("""CREATE TRIGGER fail_evidence BEFORE INSERT ON proofs
                              BEGIN SELECT RAISE(ABORT, 'simulated disk failure'); END""")
    mature = core.step(job, Calendar(mature_response()), now=100, allow=ALLOW)
    with pytest.raises(sqlite3.IntegrityError):
        store.save(revision, mature)
    assert store.load(REAL_DIGEST) == (revision, job)
    store.close()


def test_database_startup_never_creates_or_upgrades(tmp_path):
    path = tmp_path / "missing.sqlite"
    with pytest.raises(sqlite3.OperationalError):
        Store(path)
    initialize(path)
    with pytest.raises(core.StateError):
        initialize(path)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=99")
    with pytest.raises(core.StateError):
        Store(path)


def test_roundtrip_is_lossless_and_non_executable():
    for job in (initial(), submit(), real_pending_job()):
        assert core.load_job(core.dump_job(job), job.statement_digest) == job
    with pytest.raises(core.StateError):
        core.load_job(core.dump_job(initial()), b"x" * 32)


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 2),
        ("version", True),
        ("nonce", "ab"),
        ("proof", ""),
        ("work", {}),
        ("submit_urls", A),
    ],
)
def test_corrupt_state_rejected(field, value):
    data = json.loads(core.dump_job(initial()))
    data[field] = value
    with pytest.raises(core.StateError):
        core.load_job(json.dumps(data).encode(), DIGEST)


def test_duplicate_json_and_unknown_fields_rejected():
    data = core.dump_job(initial())
    for bad in (data[:-1] + b',"version":1}', data[:-1] + b',"extra":0}'):
        with pytest.raises(core.StateError):
            core.load_job(bad, DIGEST)


@pytest.mark.parametrize(
    "field,value",
    [
        ("attempts", -1),
        ("due", True),
        ("kind", "delete"),
        ("commitment", "00" * 32),
        ("error", "server said secret"),
    ],
)
def test_corrupt_work_rejected(field, value):
    data = json.loads(core.dump_job(initial()))
    data["work"][0][field] = value
    with pytest.raises(core.StateError):
        core.load_job(json.dumps(data).encode(), DIGEST)


def test_retry_saturates_without_huge_exponent_or_reset():
    retry = core.Retry(initial=2, maximum=10)
    assert [retry.delay(i) for i in range(1, 6)] == [2, 4, 8, 10, 10]
    assert retry.delay(2**31 - 1) == 10
    job = replace(initial(), work=(replace(initial().work[0], attempts=2**31 - 1),))
    failed = core.step(job, Calendar(core.CalendarError()), now=100, allow=ALLOW, retry=retry)
    assert failed.work[0].attempts == 2**31 - 1 and failed.work[0].due == 110


def test_forged_bitcoin_marker_does_not_end_pending_retries():
    job = submit()
    node = pending_node(job.submission_commitment)
    node.attestations.add(BitcoinBlockHeaderAttestation(0))
    updated = core.step(job, Calendar(raw(node)), now=100, allow=ALLOW)
    assert core.has_bitcoin(updated.proof, DIGEST)
    assert not core.verify(updated.proof, DIGEST, Chain()).cryptographically_valid
    assert updated.work[0].due is not None


def test_pending_and_unknown_are_not_bitcoin_validity():
    node = pending_node(DIGEST)
    node.attestations.add(UnknownAttestation(b"abcdefgh", b"opaque"))
    result = core.verify(core.write_proof(node), DIGEST, Chain())
    assert not result.cryptographically_valid and not result.checks
    assert result.pending_count == 1 and result.unsupported_count == 1
    assert not core.confirmation_policy(result)


def test_bad_attestation_does_not_hide_independent_valid_path():
    node = core.read_proof(REAL_PROOF, REAL_DIGEST)
    node.attestations.add(BitcoinBlockHeaderAttestation(0))  # incorrect root at file digest
    chain = Chain()
    result = core.verify(core.write_proof(node), REAL_DIGEST, chain)
    assert {check.status for check in result.checks} == {"valid", "invalid"}
    assert result.cryptographically_valid and chain.calls == [0]


def test_wrong_digest_and_trailing_bytes_fail_before_bitcoin_io():
    chain = Chain()
    for proof, digest in (
        (REAL_PROOF, DIGEST),
        (REAL_PROOF + b"x", REAL_DIGEST),
        (REAL_PROOF[:-1], REAL_DIGEST),
    ):
        with pytest.raises(core.ProofError):
            core.verify(proof, digest, chain)
    assert not chain.calls


def test_wrong_file_hash_algorithm_rejected():
    root = pending_node(b"x" * 20)
    ctx = BytesSerializationContext()
    DetachedTimestampFile(OpSHA1(), root).serialize(ctx)
    with pytest.raises(core.ProofError):
        core.verify(ctx.getbytes(), b"x" * 32, Chain())


def test_tampered_merkle_root_is_invalid_not_pending():
    node = Timestamp(DIGEST)
    node.attestations.add(BitcoinBlockHeaderAttestation(0))
    result = core.verify(core.write_proof(node), DIGEST, Chain())
    assert result.checks[0].status == "invalid"
    assert not result.cryptographically_valid


def test_rpc_unavailability_and_height_budget_not_invalid():
    node = core.read_proof(REAL_PROOF, REAL_DIGEST)
    node.attestations.add(BitcoinBlockHeaderAttestation(1))
    node.attestations.add(BitcoinBlockHeaderAttestation(2))
    chain = Chain()
    result = core.verify(core.write_proof(node), REAL_DIGEST, chain, max_heights=1)
    assert len(chain.calls) == 1
    assert any(c.reason == "verification_budget" for c in result.checks)
    absent = core.verify(REAL_PROOF, REAL_DIGEST, Chain(height=99))
    assert absent.checks[0].status == "unavailable"


def test_reorg_reverification_is_not_sticky_success():
    assert core.verify(REAL_PROOF, REAL_DIGEST, Chain()).cryptographically_valid
    altered = HEADER[:36] + b"x" * 32 + HEADER[68:]
    fresh = core.verify(REAL_PROOF, REAL_DIGEST, Chain(header=altered))
    assert fresh.checks[0].status == "invalid"
    assert not fresh.cryptographically_valid


class RPC:
    def __init__(self, *, chain="main", ibd=False, changed=False, corrupt=False):
        self.network, self.ibd = chain, ibd
        self.changed, self.corrupt = changed, corrupt
        self.calls = []

    def __call__(self, method, params):
        self.calls.append((method, params))
        if method == "getblockchaininfo":
            tip = 6 if self.changed and len(self.calls) > 1 else 5
            return {
                "chain": self.network,
                "initialblockdownload": self.ibd,
                "blocks": tip,
                "bestblockhash": "11" * 32,
            }
        if method == "getblockhash":
            return VECTOR["block_hash"]
        if method == "getblockheader":
            assert params == (VECTOR["block_hash"], False)
            return "00" * 80 if self.corrupt else HEADER.hex()
        raise AssertionError(method)


def test_bitcoin_core_port_uses_active_height_raw_header_and_stable_tip():
    rpc = RPC()
    result = core.verify(REAL_PROOF, REAL_DIGEST, core.BitcoinCore(rpc))
    assert result.cryptographically_valid
    assert result.checks[0].confirmations == 6
    assert [method for method, _ in rpc.calls] == [
        "getblockchaininfo",
        "getblockhash",
        "getblockheader",
        "getblockhash",
        "getblockchaininfo",
    ]


@pytest.mark.parametrize(
    "options",
    [
        {"chain": "regtest"},
        {"chain": "signet"},
        {"ibd": True},
        {"changed": True},
        {"corrupt": True},
    ],
)
def test_rpc_wrong_network_sync_race_and_bad_header_not_valid(options):
    result = core.verify(REAL_PROOF, REAL_DIGEST, core.BitcoinCore(RPC(**options)))
    assert result.checks[0].status == "unavailable"
    assert not result.cryptographically_valid


@pytest.mark.parametrize("minimum", [0, -1, True, 1.5, "6"])
def test_bad_confirmation_policy(minimum):
    with pytest.raises(ValueError):
        core.confirmation_policy(core.verify(REAL_PROOF, REAL_DIGEST, Chain()), minimum)


def test_tip_block_counts_as_one_confirmation():
    result = core.verify(REAL_PROOF, REAL_DIGEST, Chain(tip=0))
    assert result.checks[0].confirmations == 1
    assert core.confirmation_policy(result, 1)
    assert not core.confirmation_policy(result, 2)


def test_depth_budget_and_attestation_budget():
    root = Timestamp(DIGEST)
    tip = root
    for _ in range(core.MAX_DEPTH + 1):
        tip = tip.ops.add(OpSHA256())
    tip.attestations.add(PendingAttestation(A))
    with pytest.raises(core.ProofError):
        core.write_proof(root)
    root = pending_node(DIGEST)
    for height in range(core.MAX_ATTESTATIONS):
        root.attestations.add(BitcoinBlockHeaderAttestation(height))
    with pytest.raises(core.ProofError):
        core.write_proof(root)


def test_programming_errors_are_not_disguised_as_retryable_outages():
    with pytest.raises(RuntimeError, match="bug"):
        core.step(initial(), Calendar(RuntimeError("bug")), now=100, allow=ALLOW)
