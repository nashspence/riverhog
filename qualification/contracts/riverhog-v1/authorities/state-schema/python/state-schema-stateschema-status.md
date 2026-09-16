# state_schema.StateSchema.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateschema-status:be9002cdce -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4bba38910"></a>
- <a id="s-e03b0566eb"></a>`distribution`: `state-schema`
- <a id="s-0fb3fbe361"></a>`module`: `state_schema`
- <a id="s-06058fe80f"></a>`name`: `status`
- <a id="s-39f3e929c2"></a>`owner`: `state_schema.StateSchema`
- <a id="s-31430dc9fa"></a>`unit`: `member`

### Declared structure

- <a id="s-8fcfa41770"></a>`kind`: `"method"`
- <a id="s-d127d45b55"></a>`signature`: `"\"(self) -> 'StateStatus'\""`

## Maintained corroboration

### Related interface records

- [StateSchema](state-schema-stateschema.md)

## Governing policies

- <a id="pa-1cec351d03"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateSchema.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 27e41504f6307aaf286213c48078695f72a0ed11655fb0b2c326b730ba0516a9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'StateStatus'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "status",
  "owner": "state_schema.StateSchema",
  "unit": "member"
}
```

</details>
