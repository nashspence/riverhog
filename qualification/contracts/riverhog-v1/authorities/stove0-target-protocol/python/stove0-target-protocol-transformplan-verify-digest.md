# stove0_target_protocol.TransformPlan.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-transformplan-verify-digest:572f14bc51 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-963047bfb4"></a>
- <a id="s-e24a2dbf10"></a>`distribution`: `stove0-target-protocol`
- <a id="s-a320d081aa"></a>`module`: `stove0_target_protocol`
- <a id="s-acbdd984f4"></a>`name`: `verify_digest`
- <a id="s-8ab30cab0a"></a>`owner`: `stove0_target_protocol.TransformPlan`
- <a id="s-c3b051a768"></a>`unit`: `member`

### Declared structure

- <a id="s-47752612f0"></a>`kind`: `"method"`
- <a id="s-1d488107fd"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TransformPlan](stove0-target-protocol-transformplan.md)

## Governing policies

- <a id="pa-714b050a7d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TransformPlan.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72c659e0ee94575a9859fd21779512e811ba828ee3799e5845fea3183f132fbc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.TransformPlan",
  "unit": "member"
}
```
