# state_schema.StateConnection.begin_twophase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-begin-twophase:1c420d66ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d0b69e53a"></a>
- <a id="s-52f0440920"></a>`distribution`: `state-schema`
- <a id="s-0c276e3b52"></a>`module`: `state_schema`
- <a id="s-84a9ffed59"></a>`name`: `begin_twophase`
- <a id="s-c5efb5f8db"></a>`owner`: `state_schema.StateConnection`
- <a id="s-1fc1babd57"></a>`unit`: `member`

### Declared structure

- <a id="s-b4812c22c8"></a>`kind`: `"method"`
- <a id="s-aa07e2eb47"></a>`signature`: `"\"(self, xid: 'Optional[Any]' = None) -> 'TwoPhaseTransaction'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-a040d0617a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.begin_twophase`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 263238d0c81a2c850aa19e851ac159a793b7069914d2f802378e5794b170b7a3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, xid: 'Optional[Any]' = None) -> 'TwoPhaseTransaction'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "begin_twophase",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
