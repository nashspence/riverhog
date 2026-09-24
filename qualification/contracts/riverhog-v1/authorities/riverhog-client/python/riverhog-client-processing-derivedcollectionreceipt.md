# riverhog_client.processing.DerivedCollectionReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-derivedcollectionreceipt:4722bca25a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98579e51a4"></a>
- <a id="s-3e7f26073f"></a>`distribution`: `riverhog-client`
- <a id="s-c97f5f7fb1"></a>`module`: `riverhog_client.processing`
- <a id="s-c231ff1ff2"></a>`name`: `DerivedCollectionReceipt`
- <a id="s-bc895fb268"></a>`unit`: `export`

### Declared structure

- <a id="s-d55526cc69"></a>`kind`: `"class"`
- <a id="s-a803838ff6"></a>`signature`: `"\"(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str', derivation: 'CollectionDerivation') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-a4adbc0d83"></a>`collection_id` | `'CollectionId'` | `required` |
| <a id="s-7a66747153"></a>`archive_root_sha256` | `'str'` | `required` |
| <a id="s-7aa54da848"></a>`content_identity` | `'str'` | `required` |
| <a id="s-64cba2f772"></a>`derivation` | `'CollectionDerivation'` | `required` |

## Maintained corroboration

### Related interface records

- [from_mapping](riverhog-client-processing-derivedcollectionreceipt-from-mapping.md)
- [as_dict](riverhog-client-processing-derivedcollectionreceipt-as-dict.md)

## Governing policies

- <a id="pa-b58284bec7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.DerivedCollectionReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f92dabc003532eb4406f4c90f7bbad45133d05da46b3b1420629b2d039957b4b -->

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
  "module": "riverhog_client.processing",
  "name": "DerivedCollectionReceipt",
  "unit": "export"
}
```

</details>
