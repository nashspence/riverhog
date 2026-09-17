# state_schema.StateStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-statestatus:2ab922e6ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-127c338f48"></a>
- <a id="s-e0382f5e7e"></a>`distribution`: `state-schema`
- <a id="s-57f8e46533"></a>`module`: `state_schema`
- <a id="s-17dcadbffb"></a>`name`: `StateStatus`
- <a id="s-6ab48f9a50"></a>`unit`: `export`

### Declared structure

- <a id="s-ef6ca01db9"></a>`kind`: `"class"`
- <a id="s-78221ea4f0"></a>`signature`: `"\"(name: 'str', condition: 'StateCondition', current_revision: 'str \| None', head_revision: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-86e8a0ae89"></a>`name` | `'str'` | `required` |
| <a id="s-2208cbd498"></a>`condition` | `'StateCondition'` | `required` |
| <a id="s-147f0376af"></a>`current_revision` | `'str \| None'` | `required` |
| <a id="s-2cf3a063de"></a>`head_revision` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [as_dict](state-schema-statestatus-as-dict.md)

## Governing policies

- <a id="pa-c6e439221a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12f883507063ef9bd3ad2c6d29e50a3e8c9fbfe394e0b3c14044c34efa91b205 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "name",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "condition",
        "type": "'StateCondition'"
      },
      {
        "default": "required",
        "name": "current_revision",
        "type": "'str | None'"
      },
      {
        "default": "required",
        "name": "head_revision",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(name: 'str', condition: 'StateCondition', current_revision: 'str | None', head_revision: 'str') -> None\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "StateStatus",
  "unit": "export"
}
```

</details>
