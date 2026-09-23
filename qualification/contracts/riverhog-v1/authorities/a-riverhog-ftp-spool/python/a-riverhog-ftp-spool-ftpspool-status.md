# a_riverhog_ftp_spool.FtpSpool.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspool-status:ef3fdac0c6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4892241fb2"></a>
- <a id="s-c252a3a802"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-642490727c"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-43a10603b2"></a>`name`: `status`
- <a id="s-df72702d9a"></a>`owner`: `a_riverhog_ftp_spool.FtpSpool`
- <a id="s-80631a7e44"></a>`unit`: `member`

### Declared structure

- <a id="s-a96a06cd6e"></a>`kind`: `"method"`
- <a id="s-9c48fb5a3e"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [FtpSpool](a-riverhog-ftp-spool-ftpspool.md)

## Governing policies

- <a id="pa-fbedaaed74"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpool.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6df283bf602aa339e8026d06057164a5636053a273c36f39eeb2b08a7ee43cbf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "status",
  "owner": "a_riverhog_ftp_spool.FtpSpool",
  "unit": "member"
}
```

</details>
