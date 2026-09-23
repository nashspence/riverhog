# stove0_observer_protocol.ObserverContract.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract-seal:b2f4d58c9b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3dde7927d6"></a>
- <a id="s-d5340e3cfb"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-af126fa72e"></a>`module`: `stove0_observer_protocol`
- <a id="s-e6871f8ce7"></a>`name`: `seal`
- <a id="s-8af089c1b2"></a>`owner`: `stove0_observer_protocol.ObserverContract`
- <a id="s-f070394a56"></a>`unit`: `member`

### Declared structure

- <a id="s-5a61490e5c"></a>`kind`: `"classmethod"`
- <a id="s-872173a009"></a>`signature`: `"\"(cls, payload: 'ObserverContractPayload') -> 'ObserverContract'\""`

## Maintained corroboration

### Related interface records

- [ObserverContract](stove0-observer-protocol-observercontract.md)

## Governing policies

- <a id="pa-d9f1e4b047"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContract.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7205bb878d6fb83789a13262c7455b72b5b5df3d26db85b337213e32985b51d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ObserverContractPayload') -> 'ObserverContract'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "seal",
  "owner": "stove0_observer_protocol.ObserverContract",
  "unit": "member"
}
```

</details>
