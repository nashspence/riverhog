# stove0_core.EvaluationRecord.validate_children

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationrecord-validate-children:6e37d996d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6668a438c"></a>
- <a id="s-e203a4940c"></a>`distribution`: `stove0-server`
- <a id="s-b34f87964f"></a>`module`: `stove0_core`
- <a id="s-6a1135c88d"></a>`name`: `validate_children`
- <a id="s-f4fc56037e"></a>`owner`: `stove0_core.EvaluationRecord`
- <a id="s-ab3a036b32"></a>`unit`: `member`

### Declared structure

- <a id="s-81b3c8c816"></a>`kind`: `"method"`
- <a id="s-81e768ec24"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [EvaluationRecord](stove0-core-evaluationrecord.md)

## Governing policies

- <a id="pa-00ae33c3f7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationRecord.validate_children`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14ec2f4e5a467c7cbdecea8c35364d8b12df8111bcaa687056a3924110ce3cfc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "validate_children",
  "owner": "stove0_core.EvaluationRecord",
  "unit": "member"
}
```

</details>
