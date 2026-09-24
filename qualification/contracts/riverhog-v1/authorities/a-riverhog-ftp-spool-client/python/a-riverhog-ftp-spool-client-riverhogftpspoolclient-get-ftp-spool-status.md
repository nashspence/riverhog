# a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.get_ftp_spool_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-riverhogftpsp-ea91d59116:e7da66feda -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-48c16f6f44"></a>
- <a id="s-222540c85d"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-e15cfec0c8"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-a27f5e5cd5"></a>`name`: `get_ftp_spool_status`
- <a id="s-9120f88869"></a>`owner`: `a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient`
- <a id="s-5c0b4cda19"></a>`unit`: `member`

### Declared structure

- <a id="s-ce7994c359"></a>`kind`: `"method"`
- <a id="s-af39b797ff"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-ftp-spool status](../../a-riverhog-ftp-spool/cli/a-riverhog-ftp-spool-status.md)
- [GET /v1/status](../../a-riverhog-ftp-spool/http-operations/get-v1-status.md)
- [RiverhogFtpSpoolClient](a-riverhog-ftp-spool-client-riverhogftpspoolclient.md)

## Governing policies

- <a id="pa-f40b8ee8b8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)
- **Client method:** [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::RiverhogFtpSpoolClient.get\_ftp\_spool\_status](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py#L86)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient.get_ftp_spool_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 058cb6718210d707f4b7c6ad79a7ed7c4e0718716a09d1d711eac036f7cec44e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "get_ftp_spool_status",
  "owner": "a_riverhog_ftp_spool_client.RiverhogFtpSpoolClient",
  "unit": "member"
}
```

</details>
