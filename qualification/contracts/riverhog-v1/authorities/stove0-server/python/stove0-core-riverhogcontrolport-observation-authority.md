# stove0_core.RiverhogControlPort.observation_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-observati-9e988c9afb:6ec98ec7b6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cf213dbbfc"></a>
- <a id="s-50fc1e1ed6"></a>`distribution`: `stove0-server`
- <a id="s-73fefa3aa5"></a>`module`: `stove0_core`
- <a id="s-a0203e2812"></a>`name`: `observation_authority`
- <a id="s-17175c6656"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-e7f3a3a635"></a>`unit`: `member`

### Declared structure

- <a id="s-b5e1d52056"></a>`kind`: `"method"`
- <a id="s-803e088a91"></a>`signature`: `"\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-60d6a9e6b3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.observation_authority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47cc87e31dcae25fe423d437dda992b9ff0be1fe73981ab6f6b8748c8ae8f9c4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_authority",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```

</details>
