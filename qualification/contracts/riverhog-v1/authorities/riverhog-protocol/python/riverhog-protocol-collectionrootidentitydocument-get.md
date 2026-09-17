# riverhog_protocol.CollectionRootIdentityDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionrootidentitydocument-get:a1fa7271b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8b5aa9ad93"></a>
- <a id="s-cd7d828cb6"></a>`distribution`: `riverhog-protocol`
- <a id="s-6bfd071ea3"></a>`module`: `riverhog_protocol`
- <a id="s-b9b6017041"></a>`name`: `get`
- <a id="s-b3994fee54"></a>`owner`: `riverhog_protocol.CollectionRootIdentityDocument`
- <a id="s-6d0d564c76"></a>`unit`: `member`

### Declared structure

- <a id="s-37d9e615d3"></a>`kind`: `"method"`
- <a id="s-cfcdbf23a5"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootIdentityDocument](riverhog-protocol-collectionrootidentitydocument.md)

## Governing policies

- <a id="pa-ca4813555e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionRootIdentityDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c21638cb4c72f74af5c8f92bf0209e2ba633f141f70d2b3cee55ace3654d4a59 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.CollectionRootIdentityDocument",
  "unit": "member"
}
```

</details>
