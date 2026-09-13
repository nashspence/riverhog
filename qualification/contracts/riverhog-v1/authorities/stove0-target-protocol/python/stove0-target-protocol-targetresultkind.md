# stove0_target_protocol.TargetResultKind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetresultkind:44984a5bfe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f334439c3c"></a>
| Field | Shape |
|---|---|
| <a id="s-9019bfe398"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-c06b477244"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-430aa1b158"></a>`module` | "stove0_target_protocol" |
| <a id="s-f455bd3962"></a>`name` | "TargetResultKind" |
| <a id="s-081a73fb83"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0cc5484ad7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetResultKind`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf3985279822a9c088efffac599be22327f9e19989553741f36fed3fb1fbf0f7 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetResultKind",
  "unit": "export"
}
```
