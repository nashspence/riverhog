# stove0_core.WorkStore.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-create:d55b7eb5a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d8bed68451"></a>
- <a id="s-5580177405"></a>`distribution`: `stove0-server`
- <a id="s-30f3c2c6b8"></a>`module`: `stove0_core`
- <a id="s-2f93a30693"></a>`name`: `create`
- <a id="s-8205f61f18"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-c57b6b986e"></a>`unit`: `member`

### Declared structure

- <a id="s-234e33271e"></a>`kind`: `"method"`
- <a id="s-68d6ce5eba"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-b605113d02"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.create`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fac3aeb5607e33a1596faf2721bdd7f11290fb0ac1ce45317bcf79f20355593 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
