# a_riverhog_ftp_spool_client.FtpSpoolApiError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-ftpspoolapierror:0ee7af144e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-59885c825f"></a>
- <a id="s-846e2306e4"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-1d7f2bd00d"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-de442d6812"></a>`name`: `FtpSpoolApiError`
- <a id="s-d05afcaa9d"></a>`unit`: `export`

### Declared structure

- <a id="s-014e1497c0"></a>`kind`: `"class"`
- <a id="s-1f65af8680"></a>`signature`: `"\"(message: 'str', *, code: 'str' = 'ftp_spool_error', status: 'int \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-0575695132"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.FtpSpoolApiError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83e00b65300a07a5a336201368380fdd404c02431c8cda6e9301cc157e058971 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str' = 'ftp_spool_error', status: 'int | None' = None) -> 'None'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "FtpSpoolApiError",
  "unit": "export"
}
```

</details>
