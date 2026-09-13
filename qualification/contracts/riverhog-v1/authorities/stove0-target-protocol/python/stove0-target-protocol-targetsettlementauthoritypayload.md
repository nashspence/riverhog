# stove0_target_protocol.TargetSettlementAuthorityPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetsettlementau-61bbf99e56:636d04ed1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-350d51121d"></a>
| Field | Shape |
|---|---|
| <a id="s-fe30c2c32d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-cc6031559f"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-91700a557a"></a>`module` | "stove0_target_protocol" |
| <a id="s-e6b072b23b"></a>`name` | "TargetSettlementAuthorityPayload" |
| <a id="s-0e8755349b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a790c0249c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetSettlementAuthorityPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b4494290ac0c99e201433f00b3fac75ea060897e5e71857d98b6e3b1a7b7638 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d071532afb993c200c67017bc86e50fb52b9d8d358989eac205da3e22f0c6142",
    "signature": "\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetSettlementAuthorityPayload",
  "unit": "export"
}
```
