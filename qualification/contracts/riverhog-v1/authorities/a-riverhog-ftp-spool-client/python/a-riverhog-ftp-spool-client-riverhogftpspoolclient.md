# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpspoolclient:9b6d423f4a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4296bbbb50"></a>
- <a id="s-e565aefb8e"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-5d7b574bad"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-f5ea2e865a"></a>`name`: `RiverhogFtpSpoolClient`
- <a id="s-86a6a3d7d9"></a>`unit`: `export`

### Declared structure

- <a id="s-bee37f1bc0"></a>`kind`: `"class"`
- <a id="s-23d62a88c6"></a>`signature`: `"\"(base_url: 'str \| None' = None, token: 'str \| None' = None, *, allow_insecure_http: 'bool \| None' = None, timeout_seconds: 'float \| None' = None, http2: 'bool \| None' = None, transport: 'httpx.BaseTransport \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [__enter__](a-riverhog-ftp-spool-client-riverhogftpspoolclient-enter.md)
- [ftp_spool_health_live](a-riverhog-ftp-spool-client-riverhogftpspoolclient-ftp-spool-health-live.md)
- [flush_ftp_spool_source](a-riverhog-ftp-spool-client-riverhogftpspoolclient-flush-ftp-spool-source.md)
- [__exit__](a-riverhog-ftp-spool-client-riverhogftpspoolclient-exit.md)
- [ftp_spool_health_ready](a-riverhog-ftp-spool-client-riverhogftpspoolclient-ftp-spool-health-ready.md)
- [run_ftp_spool_pass](a-riverhog-ftp-spool-client-riverhogftpspoolclient-run-ftp-spool-pass.md)
- [close](a-riverhog-ftp-spool-client-riverhogftpspoolclient-close.md)
- [get_ftp_spool_status](a-riverhog-ftp-spool-client-riverhogftpspoolclient-get-ftp-spool-status.md)

## Governing policies

- <a id="pa-2755eec2b3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff65f0e2b3441154997dbbd572afd905e3e108c79c4b774cbe618f7ea0125a52 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str | None' = None, token: 'str | None' = None, *, allow_insecure_http: 'bool | None' = None, timeout_seconds: 'float | None' = None, http2: 'bool | None' = None, transport: 'httpx.BaseTransport | None' = None) -> 'None'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "RiverhogFtpSpoolClient",
  "unit": "export"
}
```

</details>
