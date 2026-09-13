# riverhog_provenance.PROVENANCE_OBSERVER_ENTRY_POINT_GROUP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-observer-e-28f461d8d7:0604df6d26 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e536b9e72"></a>
| Field | Shape |
|---|---|
| <a id="s-f804f70c3a"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-c77a75c3fe"></a>`distribution` | "riverhog-provenance" |
| <a id="s-570eb9f110"></a>`module` | "riverhog_provenance" |
| <a id="s-2974709116"></a>`name` | "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP" |
| <a id="s-abf0927640"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f149674a64"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_OBSERVER_ENTRY_POINT_GROUP`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ec73535cbabc099f8d99a3019227bcc798e495c3e18e0a43151f2b0940cf3ed -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog.provenance-observers"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_OBSERVER_ENTRY_POINT_GROUP",
  "unit": "export"
}
```
