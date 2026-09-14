# stove0_core.InMemoryWorkStore.load

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load:67673d8e7d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-07fe1b2c02"></a>
- <a id="s-0011549686"></a>`distribution`: `stove0-server`
- <a id="s-13b9da9a24"></a>`module`: `stove0_core`
- <a id="s-a445913c15"></a>`name`: `load`
- <a id="s-bb412b4739"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-8d85e02101"></a>`unit`: `member`

### Declared structure

- <a id="s-315767ff79"></a>`kind`: `"method"`
- <a id="s-e9e22a2bee"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-b28f4ba2d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ec4473bfd2cf86e55fc2693d1abb3701a93f59d14e41315f7e5203fdbf3903e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
