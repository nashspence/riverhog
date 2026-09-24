# riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-sourcecollectionretirem-b3d793dcc3:647e873657 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc82525832"></a>
- <a id="s-f450047545"></a>`distribution`: `riverhog-protocol`
- <a id="s-ba081d4424"></a>`module`: `riverhog_protocol`
- <a id="s-e7fb0bee0d"></a>`name`: `__getitem__`
- <a id="s-4782046dd7"></a>`owner`: `riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument`
- <a id="s-4c7ee922b0"></a>`unit`: `member`

### Declared structure

- <a id="s-5db1d90f4f"></a>`kind`: `"method"`
- <a id="s-dca0fe47f9"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [SourceCollectionRetirementClaimReferenceDocument](riverhog-protocol-sourcecollectionretirementclaimreferencedocument.md)

## Governing policies

- <a id="pa-d5e60945af"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd6a39fa34804be86a65f1c512495288217b84c5de21cd08a5530fb439d1bcf4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "__getitem__",
  "owner": "riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument",
  "unit": "member"
}
```

</details>
