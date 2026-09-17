# riverhog_protocol.RecipeIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentitydocument:a37ad0cde8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e558cea1c"></a>
- <a id="s-11ff291bb0"></a>`distribution`: `riverhog-protocol`
- <a id="s-c3590830ee"></a>`module`: `riverhog_protocol`
- <a id="s-c4425c885e"></a>`name`: `RecipeIdentityDocument`
- <a id="s-0290e175b5"></a>`unit`: `export`

### Declared structure

- <a id="s-dea766c3d8"></a>`kind`: `"class"`
- <a id="s-5f7a9a2bee"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-5dfde0eb69"></a>

- <a id="s-a85062cc06"></a>`type`: `"object"`
- <a id="s-0b669c0eee"></a>`additionalProperties`: `false`
- <a id="s-fbf9936872"></a>`required`: `["id","revision","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c96d9ae867"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bbc6de305d"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-f0f19963a7"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_identity](riverhog-protocol-recipeidentitydocument-validate-identity.md)
- [get](riverhog-protocol-recipeidentitydocument-get.md)
- [__getitem__](riverhog-protocol-recipeidentitydocument-getitem.md)

## Governing policies

- <a id="pa-b6d6c2b732"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de6a1276a15b7ba0af2cf34d27e1f7d9ab1f553417a6c37c152bd18a922bebee -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "revision": {
          "minimum": 1,
          "type": "integer"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RecipeIdentityDocument",
  "unit": "export"
}
```

</details>
