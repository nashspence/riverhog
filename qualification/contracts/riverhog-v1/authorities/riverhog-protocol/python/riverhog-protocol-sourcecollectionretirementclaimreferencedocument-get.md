# riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-sourcecollectionretirem-f9e6e86662:a09b48296d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8d3c8bda66"></a>
- <a id="s-a7da6903ba"></a>`distribution`: `riverhog-protocol`
- <a id="s-7c31785d46"></a>`module`: `riverhog_protocol`
- <a id="s-e1452ededb"></a>`name`: `get`
- <a id="s-bd66edd337"></a>`owner`: `riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument`
- <a id="s-448b932e78"></a>`unit`: `member`

### Declared structure

- <a id="s-9edb1fd728"></a>`kind`: `"method"`
- <a id="s-9327c3a499"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [SourceCollectionRetirementClaimReferenceDocument](riverhog-protocol-sourcecollectionretirementclaimreferencedocument.md)

## Governing policies

- <a id="pa-a594443aa6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5bcc7996d83428e51847e4e5cd2d11881ecccc3d13f9a3349af4ec141221aa6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.SourceCollectionRetirementClaimReferenceDocument",
  "unit": "member"
}
```

</details>
