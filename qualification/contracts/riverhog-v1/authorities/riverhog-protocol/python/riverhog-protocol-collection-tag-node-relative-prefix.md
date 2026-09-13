# riverhog_protocol.COLLECTION_TAG_NODE_RELATIVE_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-node-rel-2540c70be1:0be78fceaf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cd8304e9c3"></a>
| Field | Shape |
|---|---|
| <a id="s-ddb45367af"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-363600ba5c"></a>`distribution` | "riverhog-protocol" |
| <a id="s-bfbf5af5f5"></a>`module` | "riverhog_protocol" |
| <a id="s-54a82ea611"></a>`name` | "COLLECTION_TAG_NODE_RELATIVE_PREFIX" |
| <a id="s-cc97edc28b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-5a5f5aebb7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_TAG_NODE_RELATIVE_PREFIX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 662931ed06287975ebed4d07203be795715dddca6ededc0ba1424b93d66a6b17 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "tags/nodes"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_TAG_NODE_RELATIVE_PREFIX",
  "unit": "export"
}
```
