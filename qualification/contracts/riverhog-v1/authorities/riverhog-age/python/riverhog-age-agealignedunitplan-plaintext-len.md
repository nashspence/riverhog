# riverhog_age.AgeAlignedUnitPlan.plaintext_len

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-agealignedunitplan-plaintext-len:5a8438fc02 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67eede741b"></a>
- <a id="s-43c0a00233"></a>`distribution`: `riverhog-age`
- <a id="s-ffbe9bbcba"></a>`module`: `riverhog_age`
- <a id="s-5da201c355"></a>`name`: `plaintext_len`
- <a id="s-1a551cd34b"></a>`owner`: `riverhog_age.AgeAlignedUnitPlan`
- <a id="s-8b98990e72"></a>`unit`: `member`

### Declared structure

- <a id="s-8d5163d455"></a>`kind`: `"property"`
- <a id="s-1186339bae"></a>`signature`: `"\"(self) -> 'int'\""`

## Maintained corroboration

### Related interface records

- [AgeAlignedUnitPlan](riverhog-age-agealignedunitplan.md)

## Governing policies

- <a id="pa-22f6dd12a9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.AgeAlignedUnitPlan.plaintext_len`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 936c3e4496e1170609632d5ff35263ca2a9386d72f4eedd543f1fee364b3b33a -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'int'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "plaintext_len",
  "owner": "riverhog_age.AgeAlignedUnitPlan",
  "unit": "member"
}
```

</details>
