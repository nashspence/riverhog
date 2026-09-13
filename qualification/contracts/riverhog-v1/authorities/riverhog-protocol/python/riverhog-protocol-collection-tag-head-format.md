# riverhog_protocol.COLLECTION_TAG_HEAD_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-head-format:659a9d3d78 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1c6fac26bb"></a>
| Field | Shape |
|---|---|
| <a id="s-08ac8cc846"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-52d5253064"></a>`distribution` | "riverhog-protocol" |
| <a id="s-a883ece570"></a>`module` | "riverhog_protocol" |
| <a id="s-121fef7016"></a>`name` | "COLLECTION_TAG_HEAD_FORMAT" |
| <a id="s-3879329e54"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fe821656f1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_TAG_HEAD_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47411249c7f9261f104c346704ed06d22c7352395cb0540909a7fa401f474aca -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-collection-tag-head/v1"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_TAG_HEAD_FORMAT",
  "unit": "export"
}
```
