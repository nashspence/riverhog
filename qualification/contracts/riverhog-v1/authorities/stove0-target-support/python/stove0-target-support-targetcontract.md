# stove0_target_support.TargetContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontract:8d6698de0c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-26b04616e3"></a>
| Field | Shape |
|---|---|
| <a id="s-6e51dc39ae"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0e0b9c9e8c"></a>`distribution` | "stove0-target-support" |
| <a id="s-eb55c8879f"></a>`module` | "stove0_target_support" |
| <a id="s-50f64be519"></a>`name` | "TargetContract" |
| <a id="s-959cd9c417"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetContract.seal](stove0-target-support-targetcontract-seal.md)
- [stove0_target_support.TargetContract.support_for](stove0-target-support-targetcontract-support-for.md)
- [stove0_target_support.TargetContract.verify_digest](stove0-target-support-targetcontract-verify-digest.md)

## Governing policies

- <a id="pa-8a60bc62b7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 572367c4f3c09cd6c45b84916def0960bb1e4b3cc4278575b9e0927e308b72ea -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "444d776c4e61c75ff1edfdfeac88aa1e2ca815679fc44af9eae1992e4dde5bf0",
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetContract",
  "unit": "export"
}
```
