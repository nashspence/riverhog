# stove0_observer_support.ContentObservationResultBuilder.observed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-612d678658:6a793f6e38 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e6b8ed963"></a>
- <a id="s-9d87f491c5"></a>`distribution`: `stove0-observer-support`
- <a id="s-bc0931244d"></a>`module`: `stove0_observer_support`
- <a id="s-600e4a0f7f"></a>`name`: `observed`
- <a id="s-fca2326d35"></a>`owner`: `stove0_observer_support.ContentObservationResultBuilder`
- <a id="s-b18a47d543"></a>`unit`: `member`

### Declared structure

- <a id="s-7c232f263c"></a>`kind`: `"method"`
- <a id="s-48970a0468"></a>`signature`: `"\"(self, facts: 'Mapping[str, JsonValue]', *, execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResultBuilder](stove0-observer-support-contentobservationresultbuilder.md)

## Governing policies

- <a id="pa-89cd6d84a3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationResultBuilder.observed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1cf91264303e95aa7a6ef824a4bdb28eef73603da8d8a260fda26380acf1374 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, facts: 'Mapping[str, JsonValue]', *, execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ContentObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "observed",
  "owner": "stove0_observer_support.ContentObservationResultBuilder",
  "unit": "member"
}
```

</details>
