# stove0_core.ObserverPort.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-observerport-descriptor:9779ac9f98 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8a21aeff98"></a>
- <a id="s-12eff38d9b"></a>`distribution`: `stove0-server`
- <a id="s-440a72722e"></a>`module`: `stove0_core`
- <a id="s-25c23e73c8"></a>`name`: `descriptor`
- <a id="s-7e3907bb37"></a>`owner`: `stove0_core.ObserverPort`
- <a id="s-49fa0ffcf0"></a>`unit`: `member`

### Declared structure

- <a id="s-18210b0356"></a>`kind`: `"method"`
- <a id="s-d80f7db42f"></a>`signature`: `"\"(self, registration_id: 'str') -> 'ObserverDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ObserverPort](stove0-core-observerport.md)

## Governing policies

- <a id="pa-3be59ee67d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ObserverPort.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15eb60d65505a2d0bd2a14d7bdbd1a798cc67dbef862d9c8f6219ae8318fae41 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'ObserverDescriptor'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "descriptor",
  "owner": "stove0_core.ObserverPort",
  "unit": "member"
}
```

</details>
