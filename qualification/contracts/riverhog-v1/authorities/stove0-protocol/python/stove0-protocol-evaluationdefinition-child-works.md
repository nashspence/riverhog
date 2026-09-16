# stove0_protocol.EvaluationDefinition.child_works

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinition-child-works:fc2ea18b5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17f2dab5c0"></a>
- <a id="s-94e2eb9af3"></a>`distribution`: `stove0-protocol`
- <a id="s-c699193b58"></a>`module`: `stove0_protocol`
- <a id="s-31b46dbbc5"></a>`name`: `child_works`
- <a id="s-ebf018168b"></a>`owner`: `stove0_protocol.EvaluationDefinition`
- <a id="s-cf06a8fa4a"></a>`unit`: `member`

### Declared structure

- <a id="s-6e900e90fa"></a>`kind`: `"method"`
- <a id="s-05870783c9"></a>`signature`: `"\"(self) -> 'tuple[WorkIdentity, ...]'\""`

## Maintained corroboration

### Related interface records

- [EvaluationDefinition](stove0-protocol-evaluationdefinition.md)

## Governing policies

- <a id="pa-0e66d059e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinition.child_works`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6aec1fda30fa474fc5cb98e3a39f3de397d12f5a81a4156123d58c853667feb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'tuple[WorkIdentity, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "child_works",
  "owner": "stove0_protocol.EvaluationDefinition",
  "unit": "member"
}
```

</details>
