# stove0_core.RecipePlanner.target_input_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-target-input-selection:d53525130e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e4325ab3d7"></a>
- <a id="s-c59a94fcee"></a>`distribution`: `stove0-server`
- <a id="s-aebf211dae"></a>`module`: `stove0_core`
- <a id="s-31d9ccd04b"></a>`name`: `target_input_selection`
- <a id="s-ff2768f17c"></a>`owner`: `stove0_core.RecipePlanner`
- <a id="s-be470a2b53"></a>`unit`: `member`

### Declared structure

- <a id="s-d786b59ca7"></a>`kind`: `"method"`
- <a id="s-e92603d9b1"></a>`signature`: `"\"(self, plan: 'WorkflowPlan', selections: 'Mapping[str, ArtifactSelection]') -> 'ArtifactSelection'\""`

## Maintained corroboration

### Related interface records

- [RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-c43cc28abd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.target_input_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9ee5dec57021559ee424fb9f85379e95dc5f959c77cf776d5ad7fde16be3144 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan: 'WorkflowPlan', selections: 'Mapping[str, ArtifactSelection]') -> 'ArtifactSelection'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_input_selection",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```

</details>
