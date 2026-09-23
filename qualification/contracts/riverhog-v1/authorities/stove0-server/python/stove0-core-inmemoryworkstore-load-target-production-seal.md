# stove0_core.InMemoryWorkStore.load_target_production_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-target-6043a59488:dfdaae81a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81ce9c5056"></a>
- <a id="s-441d0abb9c"></a>`distribution`: `stove0-server`
- <a id="s-c8f0d76dec"></a>`module`: `stove0_core`
- <a id="s-39748a1e5d"></a>`name`: `load_target_production_seal`
- <a id="s-102922a4e4"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-e482d9d597"></a>`unit`: `member`

### Declared structure

- <a id="s-2ab5dc7d64"></a>`kind`: `"method"`
- <a id="s-b4c9463c33"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-01b31af699"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_target_production_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdc00e94ac4f3ab10536fa68ddf0b6408ffe039d536be0da7e2c2800e495dd4b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_production_seal",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
