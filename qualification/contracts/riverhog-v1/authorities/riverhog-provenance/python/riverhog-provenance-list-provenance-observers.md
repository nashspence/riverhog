# riverhog_provenance.list_provenance_observers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-list-provenance-observers:a8de3a5658 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67f0da2a06"></a>
- <a id="s-ee77cd7311"></a>`distribution`: `riverhog-provenance`
- <a id="s-a5f5a9087c"></a>`module`: `riverhog_provenance`
- <a id="s-75832fda8e"></a>`name`: `list_provenance_observers`
- <a id="s-dd9df186ed"></a>`unit`: `export`

### Declared structure

- <a id="s-8faa9ccfed"></a>`kind`: `"function"`
- <a id="s-eac2250408"></a>`signature`: `"\"() -> 'tuple[ProvenanceProviderMetadata, ...]'\""`

## Governing policies

- <a id="pa-31b51c9ca9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.list_provenance_observers`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 410257b1be1d7077c8c06237b086f789c7a0919e98ff7ff91d87ce1c9ba5514c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'tuple[ProvenanceProviderMetadata, ...]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "list_provenance_observers",
  "unit": "export"
}
```

</details>
