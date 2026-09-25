from __future__ import annotations

import gzip
import json
from pathlib import Path

import httpx
import pytest
from a_riverhog_opentimestamps_witness.network import BitcoinRpc, HttpCalendar
from a_riverhog_opentimestamps_witness.proof import BitcoinUnavailable, CalendarError

URL = "https://calendar.example"


def _response(
    status: int, content: bytes, *, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(status, headers=headers, stream=httpx.ByteStream(content))


def test_calendar_posts_raw_commitment_and_bounds_response() -> None:
    commitment = b"a" * 32

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            assert str(request.url) == URL + "/digest"
            assert request.content == commitment
        else:
            assert request.method == "GET"
            assert str(request.url) == URL + "/timestamp/" + commitment.hex()
        assert request.headers["accept"] == "application/vnd.opentimestamps.v1"
        assert request.headers["accept-encoding"] == "identity"
        return _response(200, b"proof")

    calendar = HttpCalendar(transport=httpx.MockTransport(handler))
    assert calendar.request("submit", URL, commitment, 100) == b"proof"
    assert calendar.request("upgrade", URL, commitment, 100) == b"proof"


@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (404, "not_found", True),
        (429, "unavailable", True),
        (500, "unavailable", True),
        (302, "rejected", False),
        (403, "rejected", False),
    ],
)
def test_calendar_statuses_are_typed(status: int, code: str, retryable: bool) -> None:
    calendar = HttpCalendar(
        transport=httpx.MockTransport(lambda _request: _response(status, b"secret"))
    )
    with pytest.raises(CalendarError) as failure:
        calendar.request("upgrade", URL, b"a" * 32, 100)
    assert (failure.value.code, failure.value.retryable) == (code, retryable)
    assert "secret" not in str(failure.value)


def test_calendar_response_budget_is_enforced_after_streaming() -> None:
    calendar = HttpCalendar(
        transport=httpx.MockTransport(lambda _request: _response(200, b"x" * 101))
    )
    with pytest.raises(CalendarError) as failure:
        calendar.request("submit", URL, b"a" * 32, 100)
    assert failure.value.code == "limit"


def test_calendar_accepts_exact_budget_and_rejects_encoded_expansion() -> None:
    compressed = gzip.compress(b"x" * (8 * 1024 * 1024))
    assert len(compressed) < 65_536
    response = _response(200, compressed, headers={"Content-Encoding": "gzip"})
    calendar = HttpCalendar(transport=httpx.MockTransport(lambda _request: response))
    with pytest.raises(CalendarError) as failure:
        calendar.request("submit", URL, b"a" * 32, 65_536)
    assert failure.value.code == "limit"

    calendar = HttpCalendar(
        transport=httpx.MockTransport(lambda _request: _response(200, b"x" * 100))
    )
    assert calendar.request("submit", URL, b"a" * 32, 100) == b"x" * 100


def test_bitcoin_rpc_uses_cookie_and_accepts_only_read_methods(tmp_path: Path) -> None:
    cookie = tmp_path / "cookie"
    cookie.write_text("__cookie__:opaque\n")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"].startswith("Basic ")
        assert request.headers["accept-encoding"] == "identity"
        body = json.loads(request.content)
        assert body["method"] == "getblockhash"
        assert body["params"] == [5]
        return _response(
            200, json.dumps({"result": "a" * 64, "error": None, "id": "riverhog-witness"}).encode()
        )

    rpc = BitcoinRpc("http://127.0.0.1:8332", cookie, transport=httpx.MockTransport(handler))
    assert rpc("getblockhash", (5,)) == "a" * 64
    with pytest.raises(BitcoinUnavailable):
        rpc("sendrawtransaction", ())
    with pytest.raises(ValueError):
        BitcoinRpc("http://remote.example:8332", cookie)


def test_bitcoin_rpc_rejects_compressed_expansion_and_bounds_raw_response(tmp_path: Path) -> None:
    cookie = tmp_path / "cookie"
    cookie.write_text("__cookie__:opaque\n")
    compressed = gzip.compress(b"x" * (8 * 1024 * 1024))
    assert len(compressed) < 65_536
    rpc = BitcoinRpc(
        "http://127.0.0.1:8332",
        cookie,
        transport=httpx.MockTransport(
            lambda _request: _response(200, compressed, headers={"Content-Encoding": "gzip"})
        ),
    )
    with pytest.raises(BitcoinUnavailable):
        rpc("getblockhash", (5,))

    empty = json.dumps(
        {"result": "", "error": None, "id": "riverhog-witness"}, separators=(",", ":")
    ).encode()
    result = "x" * (65_536 - len(empty))
    body = json.dumps(
        {"result": result, "error": None, "id": "riverhog-witness"}, separators=(",", ":")
    ).encode()
    assert len(body) == 65_536
    rpc = BitcoinRpc(
        "http://127.0.0.1:8332",
        cookie,
        transport=httpx.MockTransport(lambda _request: _response(200, body)),
    )
    assert rpc("getblockhash", (5,)) == result
    rpc = BitcoinRpc(
        "http://127.0.0.1:8332",
        cookie,
        transport=httpx.MockTransport(lambda _request: _response(200, body + b" ")),
    )
    with pytest.raises(BitcoinUnavailable):
        rpc("getblockhash", (5,))
