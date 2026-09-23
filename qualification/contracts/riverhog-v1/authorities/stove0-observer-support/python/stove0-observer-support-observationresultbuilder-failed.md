# stove0_observer_support.ObservationResultBuilder.failed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationresult-1eda5b8bf8:89fabc1c51 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e27856c103"></a>
- <a id="s-cafbabe62b"></a>`distribution`: `stove0-observer-support`
- <a id="s-2fac84d0ec"></a>`module`: `stove0_observer_support`
- <a id="s-0325efe40f"></a>`name`: `failed`
- <a id="s-0c24c7d4cc"></a>`owner`: `stove0_observer_support.ObservationResultBuilder`
- <a id="s-4139588381"></a>`unit`: `member`

### Declared structure

- <a id="s-b33c27ecc5"></a>`kind`: `"method"`
- <a id="s-2eddba4d39"></a>`signature`: `"\"(self, *, code: 'str', message: 'str', retryable: 'bool', execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ObservationResultBuilder](stove0-observer-support-observationresultbuilder.md)

## Governing policies

- <a id="pa-da8c8f2e97"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationResultBuilder.failed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0aec8395f38097d62aab84ff7bdf00a778976b9950b30028ebc6c99bb081261e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, code: 'str', message: 'str', retryable: 'bool', execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "failed",
  "owner": "stove0_observer_support.ObservationResultBuilder",
  "unit": "member"
}
```

</details>
