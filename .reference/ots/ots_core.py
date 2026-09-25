# SPDX-License-Identifier: Apache-2.0
"""Non-authoritative #870 OTS reference. No Riverhog imports or statement format.

Network ports return bytes / typed failures, never a boolean claiming validity.
Prepare and durably store a Job before step(); commit each returned Job with CAS.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, replace
from typing import Any, Protocol
from urllib.parse import urlsplit

from bitcoin.core import CBlockHeader
from opentimestamps.core.notary import (
    BitcoinBlockHeaderAttestation,
    PendingAttestation,
    VerificationError,
)
from opentimestamps.core.op import OpAppend, OpSHA256
from opentimestamps.core.serialize import (
    BytesDeserializationContext,
    BytesSerializationContext,
    DeserializationError,
)
from opentimestamps.core.timestamp import DetachedTimestampFile, Timestamp

MAX_PROOF = 65_536
MAX_STATE = 200_000
MAX_NODES = 1024
MAX_ATTESTATIONS = 128
MAX_DEPTH = 64
MAX_WORK = 128
MAX_INT = 2**63 - 1
ERROR_CODES = frozenset({"unavailable", "not_found", "rejected", "bad_proof", "limit"})


class ProofError(ValueError):
    """Malformed, unsupported, wrongly bound, or out-of-budget proof; no validity claim."""


class StateError(ValueError):
    """Corrupt or unsupported local reference state."""


class CalendarError(Exception):
    """Transport maps timeouts/404/429/5xx to retryable, permanent 4xx to False.

    Do not put server responses, credentials, or arbitrary URLs in durable diagnostics.
    """

    def __init__(self, code: str = "unavailable", *, retryable: bool = True):
        if code not in ERROR_CODES or type(retryable) is not bool:
            raise ValueError("invalid calendar failure")
        self.code, self.retryable = code, retryable
        super().__init__(code)


class BitcoinUnavailable(Exception):
    """Unavailable/untrusted/inconsistent chain observation, not an invalid OTS proof."""


class Calendar(Protocol):
    def request(self, kind: str, url: str, commitment: bytes, max_bytes: int) -> bytes:
        """One bounded exchange. submit: POST /digest; upgrade: GET /timestamp/<hex>.

        Return a serialized Timestamp subtree, NOT a detached .ots file. Enforce
        timeout, max_bytes+1 bounded read, HTTPS and no redirects in the adapter.
        Proof-supplied URLs have already passed an exact operator allowlist.
        """
        ...


def integer(value: Any, *, minimum: int = 0, maximum: int = MAX_INT) -> bool:
    return type(value) is int and minimum <= value <= maximum


def digest32(value: bytes) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise ValueError("caller must supply exactly 32 SHA-256 digest bytes")
    return value


def calendar_url(value: str) -> str:
    """Exact canonical base URL; no wildcard matching, normalization, or default hosts."""
    if type(value) is not str or not value or len(value) > 1000:
        raise ValueError("invalid calendar URL")
    PendingAttestation.check_uri(value.encode("ascii"))
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment or value.endswith("/")):
        raise ValueError("calendar needs an explicit HTTPS base URL without trailing slash")
    # Access validates malformed port syntax; authority must not be case-normalized later.
    _ = parsed.port
    return value


class _Context(BytesDeserializationContext):
    """Bound upstream parser work before it constructs an attacker-sized tree."""
    def __init__(self, data: bytes):
        if type(data) is not bytes or not 0 < len(data) <= MAX_PROOF:
            raise ProofError("proof size limit")
        super().__init__(data)
        self.reads = 0

    def read_bytes(self, expected_length: int | None = None) -> bytes:
        self.reads += 1
        if self.reads > 2 * MAX_NODES:
            raise ProofError("parser work limit")
        return super().read_bytes(expected_length)

    def read_varuint(self) -> int:
        value = 0
        for shift in range(0, 70, 7):
            byte = self.fd_read(1)[0]
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ProofError("integer encoding limit")


def nodes(root: Timestamp) -> Iterator[Timestamp]:
    stack = [(root, 0)]
    count = attestations = 0
    while stack:
        node, depth = stack.pop()
        count += 1
        attestations += len(node.attestations)
        if count > MAX_NODES or attestations > MAX_ATTESTATIONS or depth > MAX_DEPTH:
            raise ProofError("tree limit")
        for att in node.attestations:
            if isinstance(att, BitcoinBlockHeaderAttestation) and not integer(
                att.height, maximum=2**32 - 1
            ):
                raise ProofError("Bitcoin height limit")
        yield node
        stack.extend((child, depth + 1) for child in node.ops.values())


def _checked(root: Timestamp) -> Timestamp:
    for _ in nodes(root):
        pass
    return root


def read_proof(data: bytes, expected_digest: bytes) -> Timestamp:
    digest32(expected_digest)
    try:
        ctx = _Context(data)
        detached = DetachedTimestampFile.deserialize(ctx)
        ctx.assert_eof()
        if type(detached.file_hash_op) is not OpSHA256:
            raise ProofError("only caller-supplied SHA-256 statement digests are accepted")
        if detached.file_digest != expected_digest:
            raise ProofError("statement digest mismatch")
        return _checked(detached.timestamp)
    except (DeserializationError, ValueError, TypeError, RecursionError) as exc:
        raise ProofError("invalid or unsupported detached proof") from exc


def _subtree(data: bytes, commitment: bytes) -> Timestamp:
    try:
        ctx = _Context(data)
        result = Timestamp.deserialize(ctx, commitment, _recursion_limit=MAX_DEPTH + 1)
        ctx.assert_eof()
        return _checked(result)
    except (DeserializationError, ValueError, TypeError, RecursionError) as exc:
        raise ProofError("invalid or unsupported calendar proof") from exc


def write_proof(root: Timestamp) -> bytes:
    _checked(root)
    ctx = BytesSerializationContext()
    DetachedTimestampFile(OpSHA256(), root).serialize(ctx)
    data = ctx.getbytes()
    if len(data) > MAX_PROOF:
        raise ProofError("merged proof size limit")
    return data


def pending(root: Timestamp) -> set[tuple[str, bytes]]:
    return {(att.uri, node.msg) for node in nodes(root) for att in node.attestations
            if isinstance(att, PendingAttestation)}


def has_bitcoin(proof: bytes, statement_digest: bytes) -> bool:
    """Structural presence ONLY; this is not cryptographic validity or finality."""
    return any(isinstance(att, BitcoinBlockHeaderAttestation)
               for node in nodes(read_proof(proof, statement_digest))
               for att in node.attestations)


@dataclass(frozen=True)
class Retry:
    initial: int = 60
    maximum: int = 86_400

    def __post_init__(self) -> None:
        if not integer(self.initial, minimum=1) or not integer(
            self.maximum, minimum=self.initial, maximum=MAX_INT // 2
        ):
            raise ValueError("invalid retry interval")

    def delay(self, attempts: int) -> int:
        # Saturation prevents unbounded shifts even for centuries-old persisted jobs.
        return min(self.maximum, self.initial * (1 << min(max(attempts - 1, 0), 63)))


@dataclass(frozen=True)
class Work:
    kind: str
    url: str
    commitment: bytes
    attempts: int = 0
    due: int | None = 0
    error: str | None = None

    @property
    def key(self) -> tuple[str, str, bytes]:
        return self.kind, self.url, self.commitment


@dataclass(frozen=True)
class Job:
    statement_digest: bytes
    nonce: bytes
    submit_urls: tuple[str, ...]
    proof: bytes | None
    work: tuple[Work, ...]

    @property
    def submission_commitment(self) -> bytes:
        return hashlib.sha256(self.statement_digest + self.nonce).digest()

    @property
    def next_due(self) -> int | None:
        return min((w.due for w in self.work if w.due is not None), default=None)


def prepare(statement_digest: bytes, nonce: bytes, urls: tuple[str, ...], *, now: int) -> Job:
    """No I/O. Caller generates a random 32-byte nonce ONCE and persists this first."""
    digest32(statement_digest)
    digest32(nonce)
    if not integer(now) or type(urls) is not tuple or not 1 <= len(urls) <= MAX_WORK:
        raise ValueError("invalid initial job")
    if len(set(urls)) != len(urls):
        raise ValueError("duplicate calendars")
    for url in urls:
        calendar_url(url)
    commitment = hashlib.sha256(statement_digest + nonce).digest()
    return Job(statement_digest, nonce, urls, None,
               tuple(Work("submit", url, commitment, due=now) for url in urls))


def validate_job(job: Job) -> None:
    try:
        digest32(job.statement_digest)
        digest32(job.nonce)
        if (type(job.submit_urls) is not tuple or not 1 <= len(job.submit_urls) <= MAX_WORK
                or len(set(job.submit_urls)) != len(job.submit_urls)):
            raise ValueError("bad submission calendars")
        for url in job.submit_urls:
            calendar_url(url)
        tips = pending(read_proof(job.proof, job.statement_digest)) if job.proof is not None else set()
        if type(job.work) is not tuple or len(job.work) > MAX_WORK:
            raise ValueError("bad work size")
        keys = set()
        for work in job.work:
            calendar_url(work.url)
            if (type(work.commitment) is not bytes or not 1 <= len(work.commitment) <= 4096
                    or not integer(work.attempts, maximum=2**31 - 1)
                    or (work.due is not None and not integer(work.due))
                    or (work.error is not None and work.error not in ERROR_CODES)
                    or work.key in keys):
                raise ValueError("bad work")
            keys.add(work.key)
            if work.kind == "submit":
                if (work.url not in job.submit_urls
                        or work.commitment != job.submission_commitment):
                    raise ValueError("submission binding mismatch")
            elif work.kind != "upgrade" or (work.url, work.commitment) not in tips:
                raise ValueError("upgrade is not bound to a retained pending attestation")
        if job.proof is None and {w.url for w in job.work} != set(job.submit_urls):
            raise ValueError("missing unsubmitted work")
    except (ValueError, TypeError, AttributeError) as exc:
        raise StateError("invalid reference job") from exc


def _discover(job: Job, allow: frozenset[str], now: int) -> Job:
    items = {w.key: w for w in job.work}
    if job.proof:
        for url, commitment in sorted(pending(read_proof(job.proof, job.statement_digest))):
            if url in allow:
                calendar_url(url)
                key = ("upgrade", url, commitment)
                items.setdefault(key, Work(*key, due=now))
    if len(items) > MAX_WORK:
        raise ProofError("pending work limit")
    return replace(job, work=tuple(sorted(items.values(), key=lambda w: w.key)))


def step(job: Job, calendar: Calendar, *, now: int, allow: frozenset[str],
         retry: Retry = Retry()) -> Job:
    """At most ONE calendar exchange, no sleep, no clock read, no database access.

    Keep all prior evidence on failure. Pending lookups use the subnode's digest,
    never the file digest by accident. Keep retrying pending paths even after a
    Bitcoin marker appears: an attacker can forge a marker, not a valid anchor.
    """
    validate_job(job)
    if not integer(now) or type(allow) is not frozenset:
        raise ValueError("invalid scheduler inputs")
    for url in allow:
        calendar_url(url)
    job = _discover(job, allow, now)
    due = [w for w in job.work if w.due is not None and w.due <= now and w.url in allow]
    if not due:
        return job
    selected = min(due, key=lambda w: (w.due, w.attempts, w.key))
    attempts = min(selected.attempts + 1, 2**31 - 1)
    next_due = min(MAX_INT, now + retry.delay(attempts))
    updated = replace(selected, attempts=attempts, due=next_due, error=None)
    remaining = tuple(w for w in job.work if w.key != selected.key)
    try:
        raw = calendar.request(selected.kind, selected.url, selected.commitment, MAX_PROOF)
        subtree = _subtree(raw, selected.commitment)
        if job.proof:
            root = read_proof(job.proof, job.statement_digest)
        else:
            root = Timestamp(job.statement_digest)
        if selected.kind == "submit":
            target = root.ops.add(OpAppend(job.nonce)).ops.add(OpSHA256())
            target.merge(subtree)
        else:
            targets = [node for node in nodes(root) if node.msg == selected.commitment
                       and PendingAttestation(selected.url) in node.attestations]
            if not targets:
                raise StateError("pending work disappeared")
            for target in targets:
                target.merge(subtree)
        proof = write_proof(root)
        work = remaining if selected.kind == "submit" else (*remaining, updated)
        candidate = _discover(replace(job, proof=proof, work=work), allow, now)
        validate_job(candidate)
        return candidate
    except CalendarError as exc:
        updated = replace(updated, error=exc.code, due=next_due if exc.retryable else None)
    except ProofError:
        updated = replace(updated, error="bad_proof")
    return replace(job, work=tuple(sorted((*remaining, updated), key=lambda w: w.key)))


def dump_job(job: Job) -> bytes:
    validate_job(job)
    body = {"version": 1, "statement_digest": job.statement_digest.hex(),
            "nonce": job.nonce.hex(), "submit_urls": list(job.submit_urls),
            "proof": None if job.proof is None else job.proof.hex(),
            "work": [{**asdict(w), "commitment": w.commitment.hex()} for w in job.work]}
    data = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("ascii")
    if len(data) > MAX_STATE:
        raise StateError("state size limit")
    return data


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise StateError("duplicate JSON field")
        result[key] = value
    return result


def load_job(data: bytes, expected_digest: bytes) -> Job:
    digest32(expected_digest)
    if type(data) is not bytes or not 0 < len(data) <= MAX_STATE:
        raise StateError("state size limit")
    try:
        body = json.loads(data, object_pairs_hook=_unique)
        if (type(body) is not dict or set(body) != {
            "version", "statement_digest", "nonce", "submit_urls", "proof", "work"
        } or type(body["version"]) is not int or body["version"] != 1):
            raise StateError("unsupported state schema")
        if type(body["work"]) is not list or type(body["submit_urls"]) is not list:
            raise StateError("invalid state containers")
        if len(body["work"]) > MAX_WORK:
            raise StateError("work size limit")
        for item in body["work"]:
            if type(item) is not dict or set(item) != {
                "kind", "url", "commitment", "attempts", "due", "error"
            }:
                raise StateError("invalid work schema")
        work = tuple(Work(**{**w, "commitment": bytes.fromhex(w["commitment"])})
                     for w in body["work"])
        job = Job(bytes.fromhex(body["statement_digest"]), bytes.fromhex(body["nonce"]),
                  tuple(body["submit_urls"]),
                  None if body["proof"] is None else bytes.fromhex(body["proof"]), work)
        if job.statement_digest != expected_digest:
            raise StateError("persisted statement identity mismatch")
        validate_job(job)
        return job
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError) as exc:
        raise StateError("invalid reference state") from exc


@dataclass(frozen=True)
class Anchor:
    """A point-in-time active-mainnet observation from a trusted validating node."""
    height: int
    block_hash: str
    header: bytes
    tip_height: int
    tip_hash: str


class Bitcoin(Protocol):
    def anchor(self, height: int) -> Anchor: ...


def _hex(value: Any, length: int) -> bytes:
    if (type(value) is not str or len(value) != length * 2
            or any(c not in "0123456789abcdef" for c in value)):
        raise BitcoinUnavailable("malformed Bitcoin response")
    return bytes.fromhex(value)


class BitcoinCore:
    """Five RPC calls per distinct height; read-only, no wallets, no explorer trust.

    rpc(method, params) must authenticate a caller-selected fully validating
    Bitcoin Core node, bound transport time/bytes, and translate operational RPC
    errors to BitcoinUnavailable. Raw headers are not independently a chain proof.
    """
    def __init__(self, rpc: Callable[[str, tuple[Any, ...]], Any]):
        self.rpc = rpc

    def _tip(self) -> tuple[int, str]:
        info = self.rpc("getblockchaininfo", ())
        if (type(info) is not dict or info.get("chain") != "main"
                or info.get("initialblockdownload") is not False
                or not integer(info.get("blocks"))):
            raise BitcoinUnavailable("need a synchronized mainnet validating node")
        _hex(info.get("bestblockhash"), 32)
        return info["blocks"], info["bestblockhash"]

    def anchor(self, height: int) -> Anchor:
        if not integer(height, maximum=2**32 - 1):
            raise BitcoinUnavailable("invalid height")
        before = self._tip()
        if height > before[0]:
            raise BitcoinUnavailable("height unavailable")
        block_hash = self.rpc("getblockhash", (height,))
        _hex(block_hash, 32)
        raw = _hex(self.rpc("getblockheader", (block_hash, False)), 80)
        again = self.rpc("getblockhash", (height,))
        after = self._tip()
        if again != block_hash or before != after:
            raise BitcoinUnavailable("chain moved during verification")
        if hashlib.sha256(hashlib.sha256(raw).digest()).digest()[::-1].hex() != block_hash:
            raise BitcoinUnavailable("header hash mismatch")
        return Anchor(height, block_hash, raw, before[0], before[1])


@dataclass(frozen=True)
class Check:
    status: str  # valid, invalid, unavailable; never inferred from a pending marker
    height: int
    reason: str
    block_hash: str | None = None
    block_time: int | None = None
    confirmations: int | None = None
    tip_hash: str | None = None


@dataclass(frozen=True)
class Verification:
    proof_digest: str
    checks: tuple[Check, ...]
    pending_count: int
    unsupported_count: int

    @property
    def cryptographically_valid(self) -> bool:
        return any(check.status == "valid" for check in self.checks)


def verify(proof: bytes, statement_digest: bytes, bitcoin: Bitcoin, *,
           max_heights: int = 8) -> Verification:
    """Recompute from bytes each time; no sticky success across restarts/reorgs.

    A valid result is relative to the trusted node's recorded active-chain snapshot.
    It asserts existence by the block header's time, not exact creation time.
    """
    if not integer(max_heights, minimum=1, maximum=MAX_ATTESTATIONS):
        raise ValueError("invalid Bitcoin work budget")
    root = read_proof(proof, statement_digest)
    checks: list[Check] = []
    pending_count = unsupported_count = 0
    anchors: dict[int, Anchor | None] = {}
    attestations = [(node.msg, att) for node in nodes(root) for att in node.attestations]
    attestations.sort(key=lambda pair: (pair[1].TAG, pair[0], repr(pair[1])))
    for message, att in attestations:
        if isinstance(att, PendingAttestation):
            pending_count += 1
            continue
        if not isinstance(att, BitcoinBlockHeaderAttestation):
            unsupported_count += 1
            continue
        height = att.height
        if len(message) != 32:
            checks.append(Check("invalid", height, "commitment_length"))
            continue
        if height not in anchors:
            if len(anchors) >= max_heights:
                checks.append(Check("unavailable", height, "verification_budget"))
                continue
            try:
                anchors[height] = bitcoin.anchor(height)
            except BitcoinUnavailable:
                anchors[height] = None
        anchor = anchors[height]
        if anchor is None:
            checks.append(Check("unavailable", height, "chain_unavailable"))
            continue
        # Validate even a custom Bitcoin port. Mainnet consensus validation remains
        # the port's explicit trust contract; hash consistency alone is insufficient.
        if (not integer(anchor.height) or anchor.height != height or type(anchor.header) is not bytes
                or len(anchor.header) != 80 or not integer(anchor.tip_height, minimum=height)):
            checks.append(Check("unavailable", height, "invalid_anchor"))
            continue
        expected_hash = hashlib.sha256(hashlib.sha256(anchor.header).digest()).digest()[::-1]
        try:
            if _hex(anchor.block_hash, 32) != expected_hash:
                raise BitcoinUnavailable("invalid anchor hash")
            _hex(anchor.tip_hash, 32)
        except BitcoinUnavailable:
            checks.append(Check("unavailable", height, "invalid_anchor"))
            continue
        header = CBlockHeader.deserialize(anchor.header)
        try:
            block_time = att.verify_against_blockheader(message, header)
        except VerificationError:
            checks.append(Check("invalid", height, "merkle_root_mismatch", anchor.block_hash))
        else:
            checks.append(Check("valid", height, "bitcoin_merkle_commitment", anchor.block_hash,
                                block_time, anchor.tip_height - height + 1, anchor.tip_hash))
    return Verification(hashlib.sha256(proof).hexdigest(), tuple(checks),
                        pending_count, unsupported_count)


def confirmation_policy(result: Verification, minimum: int | None = None) -> bool:
    """Optional application policy. Changing minimum cannot change any Check.status."""
    if minimum is not None and not integer(minimum, minimum=1):
        raise ValueError("minimum confirmations must be a positive integer or None")
    return any(check.status == "valid" and (minimum is None or (
        check.confirmations is not None and check.confirmations >= minimum
    )) for check in result.checks)
