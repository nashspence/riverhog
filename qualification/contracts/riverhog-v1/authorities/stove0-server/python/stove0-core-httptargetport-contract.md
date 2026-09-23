# stove0_core.HttpTargetPort.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport-contract:24b43626be -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d93f1141a4"></a>
- <a id="s-128f98e0f0"></a>`distribution`: `stove0-server`
- <a id="s-675c7415af"></a>`module`: `stove0_core`
- <a id="s-8c7c61a168"></a>`name`: `contract`
- <a id="s-dd5e8339e8"></a>`owner`: `stove0_core.HttpTargetPort`
- <a id="s-114fa295db"></a>`unit`: `member`

### Declared structure

- <a id="s-940457c3d4"></a>`kind`: `"method"`
- <a id="s-8076b186ea"></a>`signature`: `"\"(self, registration_id: 'str') -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [HttpTargetPort](stove0-core-httptargetport.md)

## Governing policies

- <a id="pa-f357ad23e8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0420fee0b9256303894ddb9468914251344032e2bc5fde005f9066c5d4109c2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'TargetContract'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "contract",
  "owner": "stove0_core.HttpTargetPort",
  "unit": "member"
}
```

</details>
