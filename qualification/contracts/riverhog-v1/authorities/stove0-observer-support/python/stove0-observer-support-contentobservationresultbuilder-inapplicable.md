# stove0_observer_support.ContentObservationResultBuilder.inapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-7fb13f2a78:66100bbb0d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-374d517e7d"></a>
- <a id="s-3086c311cd"></a>`distribution`: `stove0-observer-support`
- <a id="s-0c04cd18cd"></a>`module`: `stove0_observer_support`
- <a id="s-379e8eacc2"></a>`name`: `inapplicable`
- <a id="s-94abb0293e"></a>`owner`: `stove0_observer_support.ContentObservationResultBuilder`
- <a id="s-bc245aa20e"></a>`unit`: `member`

### Declared structure

- <a id="s-db4e14d424"></a>`kind`: `"method"`
- <a id="s-6f30b42f70"></a>`signature`: `"\"(self, *, code: 'str', message: 'str', execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResultBuilder](stove0-observer-support-contentobservationresultbuilder.md)

## Governing policies

- <a id="pa-49bbd2154d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationResultBuilder.inapplicable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d93edbdcfe566d4ebc5981878a003084725c4a98c96da06c1b71871a7cadc0d3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, code: 'str', message: 'str', execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ContentObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "inapplicable",
  "owner": "stove0_observer_support.ContentObservationResultBuilder",
  "unit": "member"
}
```

</details>
