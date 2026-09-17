# stove0_core.TargetPort.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport-contract:42168f392a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-78fd642e10"></a>
- <a id="s-5e84b07f12"></a>`distribution`: `stove0-server`
- <a id="s-9b07d1dc12"></a>`module`: `stove0_core`
- <a id="s-8cbb6a5bba"></a>`name`: `contract`
- <a id="s-b9a8c9c141"></a>`owner`: `stove0_core.TargetPort`
- <a id="s-7a9b63b7db"></a>`unit`: `member`

### Declared structure

- <a id="s-3f2bf3d6d4"></a>`kind`: `"method"`
- <a id="s-aeeeb1cd73"></a>`signature`: `"\"(self, registration_id: 'str') -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [TargetPort](stove0-core-targetport.md)

## Governing policies

- <a id="pa-3ee985c03b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetPort.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34964f788c5df5904d44ceb39a5bde752b811d0f14ff08dd63b08b12f1aec22f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'TargetContract'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "contract",
  "owner": "stove0_core.TargetPort",
  "unit": "member"
}
```

</details>
