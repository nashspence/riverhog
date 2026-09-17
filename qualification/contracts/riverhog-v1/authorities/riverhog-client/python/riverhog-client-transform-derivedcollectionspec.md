# riverhog_client.transform.DerivedCollectionSpec

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollectionspec:603aa1f022 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-15ad17dac7"></a>
- <a id="s-fbb3947996"></a>`distribution`: `riverhog-client`
- <a id="s-a6bf6e02e9"></a>`module`: `riverhog_client.transform`
- <a id="s-464388594e"></a>`name`: `DerivedCollectionSpec`
- <a id="s-ab395beda6"></a>`unit`: `export`

### Declared structure

- <a id="s-f56c62a7f2"></a>`kind`: `"class"`
- <a id="s-34f4cc0430"></a>`signature`: `"\"(inputs: 'tuple[CollectionRootIdentity, ...]', recipe: 'RecipeIdentity', operation: 'OperationIdentity') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-38e75438d0"></a>`inputs` | `'tuple[CollectionRootIdentity, ...]'` | `required` |
| <a id="s-3f8357fe15"></a>`recipe` | `'RecipeIdentity'` | `required` |
| <a id="s-80f8c9d2ee"></a>`operation` | `'OperationIdentity'` | `required` |

## Governing policies

- <a id="pa-25489cfd80"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionSpec`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95fd6e25e6b2473d2b1c1dd677eb0ef084e25497cee2c988d16a8fcb0397d9be -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "inputs",
        "type": "'tuple[CollectionRootIdentity, ...]'"
      },
      {
        "default": "required",
        "name": "recipe",
        "type": "'RecipeIdentity'"
      },
      {
        "default": "required",
        "name": "operation",
        "type": "'OperationIdentity'"
      }
    ],
    "kind": "class",
    "signature": "\"(inputs: 'tuple[CollectionRootIdentity, ...]', recipe: 'RecipeIdentity', operation: 'OperationIdentity') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "DerivedCollectionSpec",
  "unit": "export"
}
```

</details>
