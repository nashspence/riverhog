# stove0_target_protocol.TargetProductionSealResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionsealresponse:9869c93ab2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f58b64da0"></a>
| Field | Shape |
|---|---|
| <a id="s-6c77ad5dad"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2f60e9ad2f"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-f7bc6d24dc"></a>`module` | "stove0_target_protocol" |
| <a id="s-144d46ffa2"></a>`name` | "TargetProductionSealResponse" |
| <a id="s-44ae8fa07d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetProductionSealResponse.validate_state](stove0-target-protocol-targetproductionsealresponse-validate-state.md)

## Governing policies

- <a id="pa-fec36f73c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionSealResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f3855260bc07b671b1453bcf3ce5465c3a82cbd3e72099a041ab71c7dfb67e6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6f7a13f1208812f0f424f11b7006ea462410222ff9b0e962c602c71eabf08e3c",
    "signature": "\"(*, state: Literal['sealing', 'sealed'], production: stove0_target_protocol.protocol.TargetProductionAuthority | None = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProductionSealResponse",
  "unit": "export"
}
```
