# a_riverhog_ftp_spool_client.FtpEventPage.require_progress_after

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-client-ftpeventpage-09665ee7ee:a8eca7f6fd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aad8433c71"></a>
- <a id="s-fdca064d7a"></a>`distribution`: `a-riverhog-ftp-spool-client`
- <a id="s-e87c7a774d"></a>`module`: `a_riverhog_ftp_spool_client`
- <a id="s-b248f3d25c"></a>`name`: `require_progress_after`
- <a id="s-0ac82f881f"></a>`owner`: `a_riverhog_ftp_spool_client.FtpEventPage`
- <a id="s-2fe7995751"></a>`unit`: `member`

### Declared structure

- <a id="s-c765a58f55"></a>`kind`: `"method"`
- <a id="s-41650bf887"></a>`signature`: `"\"(self, cursor: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [FtpEventPage](a-riverhog-ftp-spool-client-ftpeventpage.md)

## Governing policies

- <a id="pa-4f80284580"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool-client:a_riverhog_ftp_spool_client](../../../evidence/sources/authorities.md#src-3caa0330b6) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool_client.FtpEventPage.require_progress_after`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 347ddac6f4ed5db59eabdc4f820b995308ede6c6add7fe4def164c38cccb8fef -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str') -> 'None'\""
  },
  "distribution": "a-riverhog-ftp-spool-client",
  "module": "a_riverhog_ftp_spool_client",
  "name": "require_progress_after",
  "owner": "a_riverhog_ftp_spool_client.FtpEventPage",
  "unit": "member"
}
```

</details>
