# stove0_core.PreviewRiverhogPort.observation_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewriverhogport-observati-34d88382fa:cce3bd9314 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d4df22b01d"></a>
- <a id="s-fd4512aaa4"></a>`distribution`: `stove0-server`
- <a id="s-dcec504006"></a>`module`: `stove0_core`
- <a id="s-92fc8bfd5a"></a>`name`: `observation_authority`
- <a id="s-e106f0850c"></a>`owner`: `stove0_core.PreviewRiverhogPort`
- <a id="s-1b55c94a87"></a>`unit`: `member`

### Declared structure

- <a id="s-221452c417"></a>`kind`: `"method"`
- <a id="s-a7f55bf1c4"></a>`signature`: `"\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""`

## Maintained corroboration

### Related interface records

- [PreviewRiverhogPort](stove0-core-previewriverhogport.md)

## Governing policies

- <a id="pa-b8fe7b4cd9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PreviewRiverhogPort.observation_authority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c48fdc398b6763b464b2c36f1734fce44e3247af071dd4dcaf73d4776fcc400d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_authority",
  "owner": "stove0_core.PreviewRiverhogPort",
  "unit": "member"
}
```

</details>
