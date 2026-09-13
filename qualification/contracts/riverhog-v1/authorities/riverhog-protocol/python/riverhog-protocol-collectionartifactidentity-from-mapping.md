# riverhog_protocol.CollectionArtifactIdentity.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionartifactident-834dedb16a:572c939d6b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67d0ed41e0"></a>
| Field | Shape |
|---|---|
| <a id="s-65482495d6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ddf0fdefdb"></a>`distribution` | "riverhog-protocol" |
| <a id="s-a446b16a6a"></a>`module` | "riverhog_protocol" |
| <a id="s-a08d425a26"></a>`name` | "from_mapping" |
| <a id="s-b63e095f3e"></a>`owner` | "riverhog_protocol.CollectionArtifactIdentity" |
| <a id="s-55d72a5965"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionArtifactIdentity](riverhog-protocol-collectionartifactidentity.md)

## Governing policies

- <a id="pa-7e46f3b95e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionArtifactIdentity.from_mapping`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9548903aef914e0854415402814b148bea47a7cd269091a00ad171f89cf31454 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'CollectionArtifactIdentity'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.CollectionArtifactIdentity",
  "unit": "member"
}
```
