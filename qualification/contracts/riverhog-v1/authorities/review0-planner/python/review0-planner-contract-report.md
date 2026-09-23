# review0_planner.contract_report

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-planner:review0-planner-contract-report:9159ee9436 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-planner](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-756af921dc"></a>
- <a id="s-c6c6e9991d"></a>`distribution`: `review0-planner`
- <a id="s-4ba971ba4f"></a>`module`: `review0_planner`
- <a id="s-1ba5ad8076"></a>`name`: `contract_report`
- <a id="s-f05c792cd9"></a>`unit`: `export`

### Declared structure

- <a id="s-d63b7f25e9"></a>`kind`: `"function"`
- <a id="s-f5e2fca379"></a>`signature`: `"\"() -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-4c63698e48"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-planner:review0_planner](../../../evidence/sources/authorities.md#src-a8a843c072) — [some-implementations/stove0/review0/planning/src/review0\_planner/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/planning/src/review0_planner/__init__.py)

### Machine authority

- `/external_contract/python/review0_planner.contract_report`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6fa176b2aa0e29d52707261d6030626ba40e438219e7daec5593bba822657ae -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, object]'\""
  },
  "distribution": "review0-planner",
  "module": "review0_planner",
  "name": "contract_report",
  "unit": "export"
}
```

</details>
