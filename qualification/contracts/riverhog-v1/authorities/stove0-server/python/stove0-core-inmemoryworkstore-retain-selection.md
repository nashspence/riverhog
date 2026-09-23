# stove0_core.InMemoryWorkStore.retain_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-retain-selection:96a7448d68 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0a4c39b737"></a>
- <a id="s-cea921f896"></a>`distribution`: `stove0-server`
- <a id="s-9bb061fa3c"></a>`module`: `stove0_core`
- <a id="s-d2f768f06d"></a>`name`: `retain_selection`
- <a id="s-72f8363d2e"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-410a3da831"></a>`unit`: `member`

### Declared structure

- <a id="s-3358053987"></a>`kind`: `"method"`
- <a id="s-3cb20715a5"></a>`signature`: `"\"(self, selection: 'ArtifactSelection') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-e4944c752c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.retain_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe9fd86d35a6ac6943c71077197ea9bdf31eb2841a5f5e7e17b3695d1233c34a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection: 'ArtifactSelection') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retain_selection",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
