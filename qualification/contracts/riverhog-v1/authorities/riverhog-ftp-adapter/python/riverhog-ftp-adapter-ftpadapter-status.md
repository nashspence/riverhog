# riverhog_ftp_adapter.FtpAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapter-status:6d0697809e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1d88cff85e"></a>
- <a id="s-83a8eebf76"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-41fd9e7263"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-51315a0c8f"></a>`name`: `status`
- <a id="s-402b67cf40"></a>`owner`: `riverhog_ftp_adapter.FtpAdapter`
- <a id="s-7fda91e982"></a>`unit`: `member`

### Declared structure

- <a id="s-273d36808c"></a>`kind`: `"method"`
- <a id="s-bea58cf6d6"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [FtpAdapter](riverhog-ftp-adapter-ftpadapter.md)

## Governing policies

- <a id="pa-26bc9f533c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapter.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03f22676d86c6101eac64cabf5a58eeba9d098cad97bdde6a78c38f9fc88cd6e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "status",
  "owner": "riverhog_ftp_adapter.FtpAdapter",
  "unit": "member"
}
```

</details>
