# stove0_protocol.JoinSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinsettlement:abf3616a3d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dcd3714db2"></a>
| Field | Shape |
|---|---|
| <a id="s-88627ff3cc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ed5fae3176"></a>`distribution` | "stove0-protocol" |
| <a id="s-5f75ac3d4e"></a>`module` | "stove0_protocol" |
| <a id="s-d76249ec7a"></a>`name` | "JoinSettlement" |
| <a id="s-0cf8849cc9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JoinSettlement.seal](stove0-protocol-joinsettlement-seal.md)
- [stove0_protocol.JoinSettlement.verify_digest](stove0-protocol-joinsettlement-verify-digest.md)

## Governing policies

- <a id="pa-729e033f64"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinSettlement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5e63a1fe62bb8a799cfe13cf31333051a4f0e0fe224dc90b006b112081af065c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "83bf96fc750c1f70c2d908703e2f4628e73de4371e64882d7f8421d19869db32",
    "signature": "\"(*, format: Literal['stove0-join-settlement/v1'] = 'stove0-join-settlement/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinSettlement",
  "unit": "export"
}
```
