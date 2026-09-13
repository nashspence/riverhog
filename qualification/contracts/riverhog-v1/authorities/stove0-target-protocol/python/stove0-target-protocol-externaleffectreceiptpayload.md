# stove0_target_protocol.ExternalEffectReceiptPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-externaleffectreceiptpayload:2badaeee3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17fa7c8b9f"></a>
| Field | Shape |
|---|---|
| <a id="s-d7afb79751"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-405451ae6a"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-7ed4a98ec0"></a>`module` | "stove0_target_protocol" |
| <a id="s-638d9e47f0"></a>`name` | "ExternalEffectReceiptPayload" |
| <a id="s-fd6af42653"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.ExternalEffectReceiptPayload.bounded_result](stove0-target-protocol-externaleffectreceiptpayload-bounded-result.md)

## Governing policies

- <a id="pa-59854b7232"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.ExternalEffectReceiptPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e9e76a6a068786821aa02542b87e86e843693fec2da8f355f2d82e11fac652f3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d3489bc5c57692d8e98b5c61032d8c138fa5e476ccad3c14201bc96374d42f10",
    "signature": "\"(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "ExternalEffectReceiptPayload",
  "unit": "export"
}
```
