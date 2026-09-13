# riverhog_protocol.COLLECTION_UPLOAD_WORK_BATCH_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-upload-work-batch-max:6f9bd80fd9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d72f63109"></a>
| Field | Shape |
|---|---|
| <a id="s-beb0f42b19"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-e68d2873bc"></a>`distribution` | "riverhog-protocol" |
| <a id="s-56f05940ea"></a>`module` | "riverhog_protocol" |
| <a id="s-e7a0dd8b4a"></a>`name` | "COLLECTION_UPLOAD_WORK_BATCH_MAX" |
| <a id="s-19b9e17e67"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d6004e720f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_UPLOAD_WORK_BATCH_MAX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 180a9986f19f2a11042e66331f5f99543a05eecb0e3cbc5e2e81327fb02bc587 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 64
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_UPLOAD_WORK_BATCH_MAX",
  "unit": "export"
}
```
