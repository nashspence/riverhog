# stove0_protocol.CoordinationSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationsettlement:899565cadc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-07baddbb06"></a>
| Field | Shape |
|---|---|
| <a id="s-566a36adc4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7a63ff6ded"></a>`distribution` | "stove0-protocol" |
| <a id="s-9bc0c3ae17"></a>`module` | "stove0_protocol" |
| <a id="s-1f1640076f"></a>`name` | "CoordinationSettlement" |
| <a id="s-7c879305db"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.CoordinationSettlement.canonical_children](stove0-protocol-coordinationsettlement-canonical-children.md)
- [stove0_protocol.CoordinationSettlement.seal](stove0-protocol-coordinationsettlement-seal.md)
- [stove0_protocol.CoordinationSettlement.verify_contract](stove0-protocol-coordinationsettlement-verify-contract.md)

## Governing policies

- <a id="pa-fa9286ce2f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationSettlement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ad0bc026bee09c5a8728910605e728f4cbd34e0d4ad7567fc61db43fd5a2f65 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3dd77a54fcedc8cb14f2c4883a0396b657d5c6c84bb02b421866471570ac44b7",
    "signature": "\"(*, format: Literal['stove0-coordination-settlement/v1'] = 'stove0-coordination-settlement/v1', work: stove0_protocol.models.WorkIdentity, branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], children: tuple[stove0_protocol.fork_join.CoordinationChildSettlementRef, ...], contains_external_effects: bool, final_join_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, collection_result: stove0_protocol.fork_join.CoordinationCollectionResult | None = None, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationSettlement",
  "unit": "export"
}
```
