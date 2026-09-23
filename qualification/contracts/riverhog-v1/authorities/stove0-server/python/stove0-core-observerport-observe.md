# stove0_core.ObserverPort.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-observerport-observe:21a75da030 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1f0446625"></a>
- <a id="s-e6b98e0d7f"></a>`distribution`: `stove0-server`
- <a id="s-6313069873"></a>`module`: `stove0_core`
- <a id="s-251b9b17e6"></a>`name`: `observe`
- <a id="s-b3a5647691"></a>`owner`: `stove0_core.ObserverPort`
- <a id="s-a3171f1ae6"></a>`unit`: `member`

### Declared structure

- <a id="s-b195101fee"></a>`kind`: `"method"`
- <a id="s-f104042466"></a>`signature`: `"\"(self, registration_id: 'str', invocation: 'ContentObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ObserverPort](stove0-core-observerport.md)

## Governing policies

- <a id="pa-c2f11643e4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.ObserverPort.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28ca52d066bda04835964d53b4c4d0b4e3a1197638d90a721bd4b8cf2b4cf0ad -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', invocation: 'ContentObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ContentObservationResult'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observe",
  "owner": "stove0_core.ObserverPort",
  "unit": "member"
}
```

</details>
