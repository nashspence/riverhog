# riverhog_protocol.MAX_COLLECTION_TAG_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-max-collection-tag-revision:7733b3521b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d55cc63196"></a>
| Field | Shape |
|---|---|
| <a id="s-0c3adc9c9c"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-cb6dfe06dc"></a>`distribution` | "riverhog-protocol" |
| <a id="s-f57269ef7e"></a>`module` | "riverhog_protocol" |
| <a id="s-bef2f7e3e0"></a>`name` | "MAX_COLLECTION_TAG_REVISION" |
| <a id="s-8dd1c63b36"></a>`unit` | "export" |

## Governing policies

- <a id="pa-edfaa98dab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.MAX_COLLECTION_TAG_REVISION`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41fac7fd4ada68f23f84e3b44bcb7ddc87d701539b8e52a71e0d221048958e06 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 9007199254740991
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "MAX_COLLECTION_TAG_REVISION",
  "unit": "export"
}
```
