# stove0_core.HttpTargetPort.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport-preflight:30c0d6298e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-032400161c"></a>
- <a id="s-54828be333"></a>`distribution`: `stove0-server`
- <a id="s-ffdbcd04ac"></a>`module`: `stove0_core`
- <a id="s-3eb1cc98f2"></a>`name`: `preflight`
- <a id="s-2c0113e06e"></a>`owner`: `stove0_core.HttpTargetPort`
- <a id="s-fd1cdd6591"></a>`unit`: `member`

### Declared structure

- <a id="s-8e384e7c05"></a>`kind`: `"method"`
- <a id="s-24bf7747fc"></a>`signature`: `"\"(self, registration_id: 'str', request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [HttpTargetPort](stove0-core-httptargetport.md)

## Governing policies

- <a id="pa-c9921e2e6c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebcb02a8aaa6b8228f7ef68dd4e70cf70211fc712c072bad6529203ba897430b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "preflight",
  "owner": "stove0_core.HttpTargetPort",
  "unit": "member"
}
```

</details>
