# stove0_observer_support.ObserverClient.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerclient-observe:5b8ac37d95 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7075be531"></a>
- <a id="s-9ff899cf07"></a>`distribution`: `stove0-observer-support`
- <a id="s-e5937678e2"></a>`module`: `stove0_observer_support`
- <a id="s-0661dda79f"></a>`name`: `observe`
- <a id="s-5f58c3fb83"></a>`owner`: `stove0_observer_support.ObserverClient`
- <a id="s-799e62272f"></a>`unit`: `member`

### Declared structure

- <a id="s-508f13f615"></a>`kind`: `"method"`
- <a id="s-71d375ec3c"></a>`signature`: `"\"(self, invocation: 'ContentObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ObserverClient](stove0-observer-support-observerclient.md)

## Governing policies

- <a id="pa-77b147e683"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverClient.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c655a91c69a974fc83485a4f841bbf3ba263617e3af1f8d58c9a21b5e194d59e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, invocation: 'ContentObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'Any'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "observe",
  "owner": "stove0_observer_support.ObserverClient",
  "unit": "member"
}
```

</details>
