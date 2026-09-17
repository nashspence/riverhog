# riverhog_client.transform.DerivedCollectionReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollectionreceipt:3f69286bb9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-90a7606b3a"></a>
- <a id="s-32e5375a39"></a>`distribution`: `riverhog-client`
- <a id="s-3c3abc72f7"></a>`module`: `riverhog_client.transform`
- <a id="s-e084380796"></a>`name`: `DerivedCollectionReceipt`
- <a id="s-7a897c2b9c"></a>`unit`: `export`

### Declared structure

- <a id="s-5f21a0a509"></a>`kind`: `"class"`
- <a id="s-aac9f284b1"></a>`signature`: `"\"(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str', derivation: 'CollectionDerivation') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-134eaab891"></a>`collection_id` | `'CollectionId'` | `required` |
| <a id="s-68063d2f9a"></a>`archive_root_sha256` | `'str'` | `required` |
| <a id="s-8d27a52196"></a>`content_identity` | `'str'` | `required` |
| <a id="s-30caeb7169"></a>`derivation` | `'CollectionDerivation'` | `required` |

## Maintained corroboration

### Related interface records

- [from_mapping](riverhog-client-transform-derivedcollectionreceipt-from-mapping.md)
- [as_dict](riverhog-client-transform-derivedcollectionreceipt-as-dict.md)

## Governing policies

- <a id="pa-6afb225b1f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7929a80395b5ebcd2569fa2d31474b6d2f3001f0a3c40c30bb11737099fdc406 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "collection_id",
        "type": "'CollectionId'"
      },
      {
        "default": "required",
        "name": "archive_root_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "content_identity",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "derivation",
        "type": "'CollectionDerivation'"
      }
    ],
    "kind": "class",
    "signature": "\"(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str', derivation: 'CollectionDerivation') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "DerivedCollectionReceipt",
  "unit": "export"
}
```

</details>
