# stove0_target_protocol.TargetPreflightResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetpreflightresponse:97c1b950c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e11161fe1a"></a>
| Field | Shape |
|---|---|
| <a id="s-eb6dc1a93b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-dbc23250dc"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-a3e9db6ac2"></a>`module` | "stove0_target_protocol" |
| <a id="s-edd654d3be"></a>`name` | "TargetPreflightResponse" |
| <a id="s-1ef7f4c649"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetPreflightResponse.bind_protocol](stove0-target-protocol-targetpreflightresponse-bind-protocol.md)

## Governing policies

- <a id="pa-9dc63aba2f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPreflightResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c27fc67f1178485479eee1098388f26f5bdbfdb3943042e8391ba98eacddb5a -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "2de154a75245f705341e8f659ce900ce6f3b2bef59a16b22d7980249391824e9",
    "signature": "'(*, target: stove0_target_protocol.protocol.TargetContract, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetPreflightResponse",
  "unit": "export"
}
```
