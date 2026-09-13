# stove0_target_support.TargetOperationSupport

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetoperationsupport:3423f36d07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63d66a64c4"></a>
| Field | Shape |
|---|---|
| <a id="s-cb1ad2ebfd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b972c1ad89"></a>`distribution` | "stove0-target-support" |
| <a id="s-a63486b2e6"></a>`module` | "stove0_target_support" |
| <a id="s-79496fcf91"></a>`name` | "TargetOperationSupport" |
| <a id="s-ccd4bb4476"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6ad76327f4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetOperationSupport`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac08f26d3e63bc648ed693487fdf7833c21f624f901c08eec36b6ae113cede03 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6d124a4d31728580104c9df9fec11f2f903e65acc80d328b6449bf5ac33d4def",
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaDocument) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetOperationSupport",
  "unit": "export"
}
```
