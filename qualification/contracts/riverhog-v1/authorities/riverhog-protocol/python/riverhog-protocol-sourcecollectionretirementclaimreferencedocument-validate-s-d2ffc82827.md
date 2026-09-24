# riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument.validate_settlement_form

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-sourcecollectionretirem-d2ffc82827:a31cfb6dce -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9665f0f6e0"></a>
- <a id="s-263e975e1b"></a>`distribution`: `riverhog-protocol`
- <a id="s-b04966b2d3"></a>`module`: `riverhog_protocol`
- <a id="s-a8982b1153"></a>`name`: `validate_settlement_form`
- <a id="s-ee4c82f81d"></a>`owner`: `riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument`
- <a id="s-78ee0cae45"></a>`unit`: `member`

### Declared structure

- <a id="s-a513d71133"></a>`kind`: `"method"`
- <a id="s-6b581db9e8"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SourceCollectionRetirementClaimReferenceDocument](riverhog-protocol-sourcecollectionretirementclaimreferencedocument.md)

## Governing policies

- <a id="pa-ad699af962"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument.validate_settlement_form`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f104fe8a29c929e2fb220e5ae8890b698da17a82dea6dce62e343db5c0a0602 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_settlement_form",
  "owner": "riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument",
  "unit": "member"
}
```

</details>
