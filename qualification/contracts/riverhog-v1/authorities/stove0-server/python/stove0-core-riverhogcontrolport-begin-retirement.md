# stove0_core.RiverhogControlPort.begin_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-begin-retirement:893b46420c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-110561f9c2"></a>
- <a id="s-afbccb1017"></a>`distribution`: `stove0-server`
- <a id="s-b28f508e52"></a>`module`: `stove0_core`
- <a id="s-4ffb4439d8"></a>`name`: `begin_retirement`
- <a id="s-72d0705e81"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-4112e6acd5"></a>`unit`: `member`

### Declared structure

- <a id="s-daaf494b9b"></a>`kind`: `"method"`
- <a id="s-59f5936725"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-20ab4510ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.begin_retirement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0361625dae51beb035f67cc41d96d3dd26e3c807461ed324669e12068a1e8141 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_retirement",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
