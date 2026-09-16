# stove0_core.project_coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-project-coordination:47dc7e8e42 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-af843b3356"></a>
- <a id="s-a12c41affa"></a>`distribution`: `stove0-server`
- <a id="s-355e399df1"></a>`module`: `stove0_core`
- <a id="s-3ca322a063"></a>`name`: `project_coordination`
- <a id="s-b452c9b1db"></a>`unit`: `export`

### Declared structure

- <a id="s-19449faaee"></a>`kind`: `"function"`
- <a id="s-3275b73f78"></a>`signature`: `"\"(parent: 'WorkRecord', store: 'WorkStore') -> 'CoordinationProjection'\""`

## Governing policies

- <a id="pa-f6270475da"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.project_coordination`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ab69eb812fc2a19fbe9dfe3c1c9609928e46c298728f708f99fd089fa9343cc -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(parent: 'WorkRecord', store: 'WorkStore') -> 'CoordinationProjection'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "project_coordination",
  "unit": "export"
}
```

</details>
