# riverhog_protocol.CollectionDerivationDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationdocument-get:8e147de095 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7f77d994ff"></a>
- <a id="s-5e97e7ef13"></a>`distribution`: `riverhog-protocol`
- <a id="s-d5a27f03ab"></a>`module`: `riverhog_protocol`
- <a id="s-f1f36b4019"></a>`name`: `get`
- <a id="s-3ddbe35c52"></a>`owner`: `riverhog_protocol.CollectionDerivationDocument`
- <a id="s-3808df2212"></a>`unit`: `member`

### Declared structure

- <a id="s-d64791cef9"></a>`kind`: `"method"`
- <a id="s-86718cb964"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [CollectionDerivationDocument](riverhog-protocol-collectionderivationdocument.md)

## Governing policies

- <a id="pa-0d899ef31c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationDocument.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d0b64167a2f3253946c2b336fc4195d69769c3e590969779afc0d99537f46ee -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.CollectionDerivationDocument",
  "unit": "member"
}
```
