# riverhog_provenance.resolve_provenance_observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-resolve-provenance-observer:79c58b2aa8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7828013465"></a>
| Field | Shape |
|---|---|
| <a id="s-8789a80787"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b551624f01"></a>`distribution` | "riverhog-provenance" |
| <a id="s-a4d15933bb"></a>`module` | "riverhog_provenance" |
| <a id="s-2c549f76fa"></a>`name` | "resolve_provenance_observer" |
| <a id="s-afce36c300"></a>`unit` | "export" |

## Governing policies

- <a id="pa-21aa378d8c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.resolve_provenance_observer`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64e9f15166f26513a3f12d0bfb71c5df1841eea8c05236d50b7ae23a5382bdd6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(name: 'str') -> 'ResolvedProvenanceObserver'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "resolve_provenance_observer",
  "unit": "export"
}
```
