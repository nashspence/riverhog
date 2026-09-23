# stove0_observer_support.ContentObservationResultBuilder.failed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-f05d24fc6f:5b6a56b2b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-86fc23866b"></a>
- <a id="s-1a9f12ef7a"></a>`distribution`: `stove0-observer-support`
- <a id="s-966ae3ade9"></a>`module`: `stove0_observer_support`
- <a id="s-664bf4fe2b"></a>`name`: `failed`
- <a id="s-b897b65a09"></a>`owner`: `stove0_observer_support.ContentObservationResultBuilder`
- <a id="s-ad5d1470ba"></a>`unit`: `member`

### Declared structure

- <a id="s-c122dad18f"></a>`kind`: `"method"`
- <a id="s-b0ef406318"></a>`signature`: `"\"(self, *, code: 'str', message: 'str', retryable: 'bool', execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResultBuilder](stove0-observer-support-contentobservationresultbuilder.md)

## Governing policies

- <a id="pa-8a0a713081"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationResultBuilder.failed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7514834d5dd05fb42a2ee766a7da1dd898bfb63bb09e37f8aa0deb511c096e8a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, code: 'str', message: 'str', retryable: 'bool', execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ContentObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "failed",
  "owner": "stove0_observer_support.ContentObservationResultBuilder",
  "unit": "member"
}
```

</details>
