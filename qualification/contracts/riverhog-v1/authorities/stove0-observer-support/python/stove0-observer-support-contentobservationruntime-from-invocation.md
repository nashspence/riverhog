# stove0_observer_support.ContentObservationRuntime.from_invocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-9b28174bcf:01d5c1ef21 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1885f5d5f"></a>
- <a id="s-17e376a0cb"></a>`distribution`: `stove0-observer-support`
- <a id="s-d6ff451502"></a>`module`: `stove0_observer_support`
- <a id="s-96f6e14cb1"></a>`name`: `from_invocation`
- <a id="s-63c61b8855"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-8dfe1d37ef"></a>`unit`: `member`

### Declared structure

- <a id="s-c41334578c"></a>`kind`: `"classmethod"`
- <a id="s-0e6e1f5722"></a>`signature`: `"\"(cls, invocation: 'ContentObservationInvocation', *, cancellation_check: 'CancellationCheck \| None' = None, heartbeat: 'Heartbeat \| None' = None) -> 'ContentObservationRuntime'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-c86a544239"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.from_invocation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43697204a4d9c286e54f4f7c0bd6b972d0c9d3bdd25ab47494388cb0ad9a5a6b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, invocation: 'ContentObservationInvocation', *, cancellation_check: 'CancellationCheck | None' = None, heartbeat: 'Heartbeat | None' = None) -> 'ContentObservationRuntime'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "from_invocation",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
