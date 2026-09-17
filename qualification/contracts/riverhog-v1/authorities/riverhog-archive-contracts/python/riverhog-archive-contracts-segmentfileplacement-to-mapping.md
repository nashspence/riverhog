# riverhog_archive_contracts.SegmentFilePlacement.to_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-segmentfilepla-adead3dbf3:c28b1316b6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4c1645596"></a>
- <a id="s-1fa88e57de"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-910e409760"></a>`module`: `riverhog_archive_contracts`
- <a id="s-043855c74c"></a>`name`: `to_mapping`
- <a id="s-20bd7fd24b"></a>`owner`: `riverhog_archive_contracts.SegmentFilePlacement`
- <a id="s-8768bf0b22"></a>`unit`: `member`

### Declared structure

- <a id="s-fe48bdd812"></a>`kind`: `"method"`
- <a id="s-5f061328fb"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [SegmentFilePlacement](riverhog-archive-contracts-segmentfileplacement.md)

## Governing policies

- <a id="pa-7241e04896"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.SegmentFilePlacement.to_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b01859bdc85c59f0f3531d04c51966ebdaeca70c36c0b08f0fb2e5ff57542aca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_mapping",
  "owner": "riverhog_archive_contracts.SegmentFilePlacement",
  "unit": "member"
}
```

</details>
