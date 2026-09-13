# stove0_target_protocol.TargetExecutionEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetexecutionevidence:7356d35ad7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7531911f3b"></a>
| Field | Shape |
|---|---|
| <a id="s-d1ef3b30fa"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b118899cef"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-981d91cee3"></a>`module` | "stove0_target_protocol" |
| <a id="s-1ed001e35d"></a>`name` | "TargetExecutionEvidence" |
| <a id="s-2567df8b5b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1ef31fa90a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetExecutionEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9144e2529751b18758189507b7b958dbef91336634bb70b05b19082de84ecd5c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "f76617128b8960e01b2b657a4031d97d1632c188c092473b906be914186ef931",
    "signature": "\"(*, target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], runtime: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetExecutionEvidence",
  "unit": "export"
}
```
