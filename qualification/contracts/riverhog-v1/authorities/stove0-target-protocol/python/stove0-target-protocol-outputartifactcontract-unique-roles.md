# stove0_target_protocol.OutputArtifactContract.unique_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactcont-16611f760e:e64fd51d43 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-27b6436623"></a>
| Field | Shape |
|---|---|
| <a id="s-145426d0c4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ae16108077"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-be39d42bd0"></a>`module` | "stove0_target_protocol" |
| <a id="s-7c7d08f5ea"></a>`name` | "unique_roles" |
| <a id="s-215c3914c2"></a>`owner` | "stove0_target_protocol.OutputArtifactContract" |
| <a id="s-74a39db4f0"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifactContract](stove0-target-protocol-outputartifactcontract.md)

## Governing policies

- <a id="pa-da1ff74faa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactContract.unique_roles`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5850debed826ee0a21d84a1ebd53e1fc4cf0f6700327ba29975bc093b96b756f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "unique_roles",
  "owner": "stove0_target_protocol.OutputArtifactContract",
  "unit": "member"
}
```
