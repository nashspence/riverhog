# stove0_target_support.TargetPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetplan:f001c945cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-71436744c2"></a>
- <a id="s-461d2ceaad"></a>`distribution`: `stove0-target-support`
- <a id="s-b50199dba3"></a>`module`: `stove0_target_support`
- <a id="s-e23d5b9b9c"></a>`name`: `TargetPlan`
- <a id="s-676529f819"></a>`unit`: `export`

### Declared structure

- <a id="s-8c2d070cf2"></a>`kind`: `"object"`
- <a id="s-862ee57ce3"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-d5e3170215"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8534caf2442224d76f4dfeb066163a0de2203e061d807962abe55002d7042002 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetPlan",
  "unit": "export"
}
```

</details>
