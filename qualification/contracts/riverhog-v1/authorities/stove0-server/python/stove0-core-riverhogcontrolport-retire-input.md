# stove0_core.RiverhogControlPort.retire_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-retire-input:4b4bf8515c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f8671ee4ff"></a>
- <a id="s-4dcd06aad5"></a>`distribution`: `stove0-server`
- <a id="s-bfb02cc95c"></a>`module`: `stove0_core`
- <a id="s-6399912955"></a>`name`: `retire_input`
- <a id="s-17a5e16954"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-6083f69d39"></a>`unit`: `member`

### Declared structure

- <a id="s-e144e4c0e8"></a>`kind`: `"method"`
- <a id="s-edb6f0f03d"></a>`signature`: `"\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-7ead79126c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.retire_input`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d177df0f2ed94a56268e3fbf88ab20ab8600b57b93442f498892c21e6731a101 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retire_input",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
