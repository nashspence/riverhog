# stove0_protocol.TargetPlanBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-targetplanbinding:9d47f60f15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bb0bfe0a80"></a>
| Field | Shape |
|---|---|
| <a id="s-8036fa02db"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8d90bc3a71"></a>`distribution` | "stove0-protocol" |
| <a id="s-c58304c634"></a>`module` | "stove0_protocol" |
| <a id="s-0a1435c375"></a>`name` | "TargetPlanBinding" |
| <a id="s-8521e115de"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.TargetPlanBinding.require_plan_document](stove0-protocol-targetplanbinding-require-plan-document.md)

## Governing policies

- <a id="pa-9bb5474ee9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.TargetPlanBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6892df1bfb07cffe7e0d617dd5ea734a8fbd9277329c0591f6092cc601c815c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7682f9c07f20e2427e0662961cc33879f27f42dfe6ab6aa84eba726de0c6803e",
    "signature": "\"(*, protocol: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan: dict[str, JsonValue], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "TargetPlanBinding",
  "unit": "export"
}
```
