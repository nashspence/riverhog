# riverhog_client.processing.DerivedCollectionSpec

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-derivedcollectionspec:46db62883b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67700a138e"></a>
- <a id="s-defdb79320"></a>`distribution`: `riverhog-client`
- <a id="s-cfadc627f7"></a>`module`: `riverhog_client.processing`
- <a id="s-e80b34ff81"></a>`name`: `DerivedCollectionSpec`
- <a id="s-9cb48584ac"></a>`unit`: `export`

### Declared structure

- <a id="s-8cfbbdc689"></a>`kind`: `"class"`
- <a id="s-a501591ee4"></a>`signature`: `"\"(inputs: 'tuple[CollectionRootIdentity, ...]', recipe: 'RecipeIdentity', operation: 'OperationIdentity') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-93dd5a99eb"></a>`inputs` | `'tuple[CollectionRootIdentity, ...]'` | `required` |
| <a id="s-002191b16d"></a>`recipe` | `'RecipeIdentity'` | `required` |
| <a id="s-3b15bef371"></a>`operation` | `'OperationIdentity'` | `required` |

## Governing policies

- <a id="pa-fb9f67f766"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.DerivedCollectionSpec`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b26954e3331ba114fe197ce788a3e38e5dc1af7c97e678a777e107ec1dec52a8 -->

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
  "module": "riverhog_client.processing",
  "name": "DerivedCollectionSpec",
  "unit": "export"
}
```

</details>
