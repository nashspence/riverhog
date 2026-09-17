# riverhog_protocol.RecipeIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentity:9ec6833f54 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c0d6abd237"></a>
- <a id="s-e89ac4a521"></a>`distribution`: `riverhog-protocol`
- <a id="s-9d747aab51"></a>`module`: `riverhog_protocol`
- <a id="s-3749518bb8"></a>`name`: `RecipeIdentity`
- <a id="s-c31517471a"></a>`unit`: `export`

### Declared structure

- <a id="s-ebb796681b"></a>`kind`: `"class"`
- <a id="s-81ba5c61ea"></a>`signature`: `"\"(id: 'str', revision: 'int', sha256: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-8f34cd2b16"></a>`id` | `'str'` | `required` |
| <a id="s-75b8abd798"></a>`revision` | `'int'` | `required` |
| <a id="s-35285abe0a"></a>`sha256` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [as_dict](riverhog-protocol-recipeidentity-as-dict.md)
- [from_mapping](riverhog-protocol-recipeidentity-from-mapping.md)

## Governing policies

- <a id="pa-5ab58d2f72"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1152ad725f76e6840e8ea8abc4b3e584d941229c764425b9e7b2fcc7c14a4f8 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "revision",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(id: 'str', revision: 'int', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RecipeIdentity",
  "unit": "export"
}
```

</details>
