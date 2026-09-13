# stove0_target_support.ExternalEffectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-externaleffectreceipt:70110147a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7398b0c175"></a>
| Field | Shape |
|---|---|
| <a id="s-67bfc74757"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fc89aae5d5"></a>`distribution` | "stove0-target-support" |
| <a id="s-638fef1efb"></a>`module` | "stove0_target_support" |
| <a id="s-a883c0abd6"></a>`name` | "ExternalEffectReceipt" |
| <a id="s-c819c73f09"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.ExternalEffectReceipt.verify_digest](stove0-target-support-externaleffectreceipt-verify-digest.md)
- [stove0_target_support.ExternalEffectReceipt.seal](stove0-target-support-externaleffectreceipt-seal.md)

## Governing policies

- <a id="pa-5808dbee77"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.ExternalEffectReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14cac6c57900ac79cb47f38338a335bfd32a929be9c202f0f0bd5ee94a488ed5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d35b7a54eb7db5847253bd3315189fea8ca2a702b3516d1e794dca0d3c880621",
    "signature": "\"(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue], receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "ExternalEffectReceipt",
  "unit": "export"
}
```
