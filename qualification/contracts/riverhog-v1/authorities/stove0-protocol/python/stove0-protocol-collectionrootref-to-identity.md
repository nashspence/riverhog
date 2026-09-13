# stove0_protocol.CollectionRootRef.to_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-collectionrootref-to-identity:f0aee3447d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c47dac668c"></a>
| Field | Shape |
|---|---|
| <a id="s-10c61eddd7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d33f96e792"></a>`distribution` | "stove0-protocol" |
| <a id="s-1fc3cc4482"></a>`module` | "stove0_protocol" |
| <a id="s-5f58dbf0f5"></a>`name` | "to_identity" |
| <a id="s-65121453fd"></a>`owner` | "stove0_protocol.CollectionRootRef" |
| <a id="s-db516929e3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.CollectionRootRef](stove0-protocol-collectionrootref.md)

## Governing policies

- <a id="pa-1be687e222"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CollectionRootRef.to_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 054cc39424998632ce1d12b9a697330447c918551c078045dde3d3a9e39f3780 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CollectionRootIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "to_identity",
  "owner": "stove0_protocol.CollectionRootRef",
  "unit": "member"
}
```
