# riverhog_ftp_adapter.FtpAdapter.run_once

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapter-run-once:b9eb2da60b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2936e1008a"></a>
- <a id="s-ac1be25d36"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-7d48bebfe6"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-309c6dd74f"></a>`name`: `run_once`
- <a id="s-e66a914066"></a>`owner`: `riverhog_ftp_adapter.FtpAdapter`
- <a id="s-404c8871a2"></a>`unit`: `member`

### Declared structure

- <a id="s-936429b51f"></a>`kind`: `"method"`
- <a id="s-a83034dd19"></a>`signature`: `"\"(self, source_ids: 'Sequence[str] \| None' = None) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [FtpAdapter](riverhog-ftp-adapter-ftpadapter.md)

## Governing policies

- <a id="pa-fe6d3635c0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapter.run_once`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f56c7c1f24476e8b67031f1cf9541607e1d5466cfefd2b84721abec46acb642f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_ids: 'Sequence[str] | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "run_once",
  "owner": "riverhog_ftp_adapter.FtpAdapter",
  "unit": "member"
}
```
