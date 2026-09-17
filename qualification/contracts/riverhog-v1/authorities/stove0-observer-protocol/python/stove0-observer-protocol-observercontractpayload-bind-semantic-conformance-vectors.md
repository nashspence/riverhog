# stove0_observer_protocol.ObserverContractPayload.bind_semantic_conformance_vectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract-fc6943c7e8:3b1ad4337a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b514907e4"></a>
- <a id="s-6a3326a210"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-533ed21e6e"></a>`module`: `stove0_observer_protocol`
- <a id="s-c910e9bc1a"></a>`name`: `bind_semantic_conformance_vectors`
- <a id="s-d7d974aa5b"></a>`owner`: `stove0_observer_protocol.ObserverContractPayload`
- <a id="s-d15ec1d122"></a>`unit`: `member`

### Declared structure

- <a id="s-62fb82ad81"></a>`kind`: `"method"`
- <a id="s-c19262127f"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ObserverContractPayload](stove0-observer-protocol-observercontractpayload.md)

## Governing policies

- <a id="pa-80191fb417"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContractPayload.bind_semantic_conformance_vectors`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de51a57e06c67b28e62dc12716769f590a5927b20b30c58445d4445057365743 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "bind_semantic_conformance_vectors",
  "owner": "stove0_observer_protocol.ObserverContractPayload",
  "unit": "member"
}
```

</details>
