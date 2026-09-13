# stove0_target_support.TargetDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdeclaration:b23862c5f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2a0da35103"></a>
| Field | Shape |
|---|---|
| <a id="s-c2e9d4e54a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d6ef936742"></a>`distribution` | "stove0-target-support" |
| <a id="s-855846da2e"></a>`module` | "stove0_target_support" |
| <a id="s-a98d65cb81"></a>`name` | "TargetDeclaration" |
| <a id="s-69b871ce2c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8c06cfcc9c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8f7a73c451ce9784e15e1f7d5b0e4257e9b11b1a8308dba371e5ecba59162a8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "2799fc17c1e3def4ba574b3d4ac58984d664b817f1a646a63e4f35c7343b6997",
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetDeclaration",
  "unit": "export"
}
```
