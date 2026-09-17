# state_schema.StateConnection.invalidated

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-invalidated:76c36b7ec4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ad1ef27a8"></a>
- <a id="s-5a473eb6fe"></a>`distribution`: `state-schema`
- <a id="s-7c50d2657a"></a>`module`: `state_schema`
- <a id="s-8b43998baf"></a>`name`: `invalidated`
- <a id="s-6ddf6124f6"></a>`owner`: `state_schema.StateConnection`
- <a id="s-26ef7e7269"></a>`unit`: `member`

### Declared structure

- <a id="s-dbda97db65"></a>`kind`: `"property"`
- <a id="s-9699a644a7"></a>`signature`: `"\"(self) -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-c3c91c5149"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.invalidated`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e46bc0563ccf9dea381f21be7065e9f8de5133665132485a6868b800fdbfc3de -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "invalidated",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
