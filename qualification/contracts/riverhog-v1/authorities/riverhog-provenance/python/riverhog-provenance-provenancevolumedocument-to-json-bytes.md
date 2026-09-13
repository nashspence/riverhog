# riverhog_provenance.ProvenanceVolumeDocument.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancevolumedocum-ef6c7917ae:c9402eb530 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91f76bbd8f"></a>
| Field | Shape |
|---|---|
| <a id="s-843ed0b30b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9f26f9cff2"></a>`distribution` | "riverhog-provenance" |
| <a id="s-2ca1a56698"></a>`module` | "riverhog_provenance" |
| <a id="s-2761dd1acb"></a>`name` | "to_json_bytes" |
| <a id="s-1f7638859e"></a>`owner` | "riverhog_provenance.ProvenanceVolumeDocument" |
| <a id="s-6132fa85c4"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ProvenanceVolumeDocument](riverhog-provenance-provenancevolumedocument.md)

## Governing policies

- <a id="pa-fbebcb47f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceVolumeDocument.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80d5fe0876963ceb10e5d81ee767c9e44034f544156fc86a7e4999f7bab6a0dd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "to_json_bytes",
  "owner": "riverhog_provenance.ProvenanceVolumeDocument",
  "unit": "member"
}
```
