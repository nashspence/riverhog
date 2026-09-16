# state_schema.StateStatus.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-statestatus-as-dict:859f8b557b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5933869d09"></a>
- <a id="s-c610dd0e66"></a>`distribution`: `state-schema`
- <a id="s-537d473c51"></a>`module`: `state_schema`
- <a id="s-922dbf2dc6"></a>`name`: `as_dict`
- <a id="s-b9c446c73f"></a>`owner`: `state_schema.StateStatus`
- <a id="s-d213e24078"></a>`unit`: `member`

### Declared structure

- <a id="s-a96207305e"></a>`kind`: `"method"`
- <a id="s-bb992a37e7"></a>`signature`: `"\"(self) -> 'dict[str, str \| None]'\""`

## Maintained corroboration

### Related interface records

- [StateStatus](state-schema-statestatus.md)

## Governing policies

- <a id="pa-4f557f223e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateStatus.as_dict`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17fb57b6a9400e4e2e93cf8481abc15a82725c02695cf97281db7f19dd9956d6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str | None]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "as_dict",
  "owner": "state_schema.StateStatus",
  "unit": "member"
}
```

</details>
