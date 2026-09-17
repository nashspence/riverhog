# state_schema.StateConnection.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-exit:1437c7bea1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b868f1471"></a>
- <a id="s-d62a04acab"></a>`distribution`: `state-schema`
- <a id="s-1fddad8875"></a>`module`: `state_schema`
- <a id="s-248a7c43a3"></a>`name`: `__exit__`
- <a id="s-1defcbbd7d"></a>`owner`: `state_schema.StateConnection`
- <a id="s-f4f367f44e"></a>`unit`: `member`

### Declared structure

- <a id="s-ee9963323b"></a>`kind`: `"method"`
- <a id="s-a227c9715c"></a>`signature`: `"\"(self, type_: 'Any', value: 'Any', traceback: 'Any') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-08e54729fd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90bb6cc64048dea705e2327f2b51ed39b3eba5b91ea2cef0e937575c0d873cce -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, type_: 'Any', value: 'Any', traceback: 'Any') -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "__exit__",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
