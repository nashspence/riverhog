# stove0_core.InMemoryWorkStore.iter_target_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-iter-target-963c3949c6:6fc3cb975f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bc7f9abf86"></a>
- <a id="s-d1f8d0d9fa"></a>`distribution`: `stove0-server`
- <a id="s-5eb55e0a17"></a>`module`: `stove0_core`
- <a id="s-51cda17b77"></a>`name`: `iter_target_dispositions`
- <a id="s-ba87bc131d"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-db63a06f23"></a>`unit`: `member`

### Declared structure

- <a id="s-7c1cc6dd56"></a>`kind`: `"method"`
- <a id="s-9f34c4fe5b"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-dac6031d1f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.iter_target_dispositions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c965fc9110e025b4b556bb25152fe2c2edb536971ead20a23db2453e3b45db5d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_dispositions",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
