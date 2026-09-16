# riverhog_protocol.validate_collection_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-validate-collection-id:25203a007b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-964af2d77d"></a>
- <a id="s-7eb201d2a7"></a>`distribution`: `riverhog-protocol`
- <a id="s-b8591bbe63"></a>`module`: `riverhog_protocol`
- <a id="s-ecff2c5ab0"></a>`name`: `validate_collection_id`
- <a id="s-c8e1d304a6"></a>`unit`: `export`

### Declared structure

- <a id="s-92780c0af2"></a>`kind`: `"function"`
- <a id="s-c33f3237ce"></a>`signature`: `"\"(value: 'object') -> 'int'\""`

## Governing policies

- <a id="pa-3ecd26f4c8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.validate_collection_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84a2e1afb348e0c76dd5afd6af8fbbc920fc94cfc59f827fc225fddc5b5aabe6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'int'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_collection_id",
  "unit": "export"
}
```

</details>
