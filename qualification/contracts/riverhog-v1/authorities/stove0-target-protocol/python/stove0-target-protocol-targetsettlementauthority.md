# stove0_target_protocol.TargetSettlementAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetsettlementauthority:ff60d0a891 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1ee207cf0"></a>
| Field | Shape |
|---|---|
| <a id="s-f35753aa1a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fba0b778d8"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-6f0ed47037"></a>`module` | "stove0_target_protocol" |
| <a id="s-2f38e4e74e"></a>`name` | "TargetSettlementAuthority" |
| <a id="s-b4bacbc001"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetSettlementAuthority.verify_digest](stove0-target-protocol-targetsettlementauthority-verify-digest.md)
- [stove0_target_protocol.TargetSettlementAuthority.seal](stove0-target-protocol-targetsettlementauthority-seal.md)

## Governing policies

- <a id="pa-cff6f2da7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetSettlementAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86a81b603ce31319f323c485bdeeb13d7e52143b6d4c024c4fc05f94ea345748 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "27d5c82cd51c2f39dfd46a39cd6be0c7ba647be6162e468f45697fca4d1964de",
    "signature": "\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetSettlementAuthority",
  "unit": "export"
}
```
