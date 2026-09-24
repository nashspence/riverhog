# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.list_ftp_spool_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-272dde6c48:7a4067ae72 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-87ed17dee9"></a>
- <a id="s-2944e1ce8a"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-7619d917e8"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-33e53c3474"></a>`name`: `list_ftp_spool_events`
- <a id="s-0a605aec9f"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-83bd8648b1"></a>`unit`: `member`

### Declared structure

- <a id="s-b5d810a074"></a>`kind`: `"method"`
- <a id="s-cc3826b7b8"></a>`signature`: `"\"(self, source_id: 'str', *, after: 'str \| None' = None, limit: 'int' = 100) -> 'FtpEventPage'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool events](../../a-riverhog-ftp-spool/cli/a-riverhog-ftp-spool-events.md)
- [GET /v1/sources/{source_id}/events](../../a-riverhog-ftp-spool/http-operations/get-v1-sources-source-id-events.md)
- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-ed38ca7bdc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)
- **Client method:** [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::RiverhogFtpSpoolClient.list\_ftp\_spool\_events](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py#L100)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.list_ftp_spool_events`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f828a57afb9d5c64f949e7813169b1e03cc6df5a6b41f9eff31d66eacc913e63 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str', *, after: 'str | None' = None, limit: 'int' = 100) -> 'FtpEventPage'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "list_ftp_spool_events",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
