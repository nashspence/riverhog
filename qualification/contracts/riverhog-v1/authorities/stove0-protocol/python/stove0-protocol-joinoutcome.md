# stove0_protocol.JoinOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinoutcome:b3ceab1c51 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e1bc549b7f"></a>
| Field | Shape |
|---|---|
| <a id="s-87adc2cf7e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-95f27f9801"></a>`distribution` | "stove0-protocol" |
| <a id="s-8326d5c807"></a>`module` | "stove0_protocol" |
| <a id="s-bafcd7c452"></a>`name` | "JoinOutcome" |
| <a id="s-2ed833221d"></a>`unit` | "export" |

## Governing policies

- <a id="pa-5e139fc0e9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinOutcome`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 068593669ba98be2dd2e5e75db26a2a36b5a4df12df081e4b54c86c1f040b952 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8245153ec249680ee6fa42b10141c4f16e28f89bb7863c1baf10dbdcd336d1fe",
    "signature": "\"(*, format: Literal['stove0-join-outcome/v1'] = 'stove0-join-outcome/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['failed', 'inapplicable', 'interrupted', 'canceled']) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinOutcome",
  "unit": "export"
}
```
