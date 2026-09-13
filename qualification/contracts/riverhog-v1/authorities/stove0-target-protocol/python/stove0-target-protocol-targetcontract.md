# stove0_target_protocol.TargetContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontract:cd5dbf5d5d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d5596ca47f"></a>
| Field | Shape |
|---|---|
| <a id="s-0db1e90965"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b56dc1bb94"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-68f1106cf9"></a>`module` | "stove0_target_protocol" |
| <a id="s-8f8898d0f4"></a>`name` | "TargetContract" |
| <a id="s-b1c40daa90"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetContract.seal](stove0-target-protocol-targetcontract-seal.md)
- [stove0_target_protocol.TargetContract.support_for](stove0-target-protocol-targetcontract-support-for.md)
- [stove0_target_protocol.TargetContract.verify_digest](stove0-target-protocol-targetcontract-verify-digest.md)

## Governing policies

- <a id="pa-6f1207d56f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 252e41c978ee8d62e02d806d9d2d693c2a320fd1fd9383ad629dbf30cc32b712 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "444d776c4e61c75ff1edfdfeac88aa1e2ca815679fc44af9eae1992e4dde5bf0",
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetContract",
  "unit": "export"
}
```
