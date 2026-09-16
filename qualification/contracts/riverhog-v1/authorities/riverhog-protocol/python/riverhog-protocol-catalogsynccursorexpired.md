# riverhog_protocol.CatalogSyncCursorExpired

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsynccursorexpired:714ffef248 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31689ccfd6"></a>
- <a id="s-6ecc9869c1"></a>`distribution`: `riverhog-protocol`
- <a id="s-a1f0c9b753"></a>`module`: `riverhog_protocol`
- <a id="s-22d5740aeb"></a>`name`: `CatalogSyncCursorExpired`
- <a id="s-fa92b63555"></a>`unit`: `export`

### Declared structure

- <a id="s-c73675fd29"></a>`kind`: `"class"`
- <a id="s-3152b7ee14"></a>`signature`: `"\"(message: 'str', *, code: 'str \| None' = None, observed_status: 'int \| None' = None, details: 'dict[str, Any] \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-5c08980da0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncCursorExpired`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 756fad96b0d8f281f104e793592e4c4a8b265933113006eed9989589f7ec6021 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncCursorExpired",
  "unit": "export"
}
```

</details>
