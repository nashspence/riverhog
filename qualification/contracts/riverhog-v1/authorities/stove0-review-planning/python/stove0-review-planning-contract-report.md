# stove0_review_planning.contract_report

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-planning:stove0-review-planning-contract-report:55ffc7c4f5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-daee5d59ca"></a>
- <a id="s-55fed2e22f"></a>`distribution`: `stove0-review-planning`
- <a id="s-fb669c2d50"></a>`module`: `stove0_review_planning`
- <a id="s-49db3157b3"></a>`name`: `contract_report`
- <a id="s-0d34f77d39"></a>`unit`: `export`

### Declared structure

- <a id="s-886f0a6587"></a>`kind`: `"function"`
- <a id="s-cd25b7da0b"></a>`signature`: `"\"() -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-70a712b4f1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-planning:stove0_review_planning](../../../evidence/sources/authorities.md#src-354ae519e9) — [reference/stove0/targets/review/planning/src/stove0\_review\_planning/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/planning/src/stove0_review_planning/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_planning.contract_report`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1ae7ba951af337f4d794e95d7a067fea1b8fb258d6052e64b426d5408d70150 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, object]'\""
  },
  "distribution": "stove0-review-planning",
  "module": "stove0_review_planning",
  "name": "contract_report",
  "unit": "export"
}
```

</details>
