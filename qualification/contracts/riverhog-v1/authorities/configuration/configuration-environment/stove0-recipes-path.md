# STOVE0_RECIPES_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-recipes-path:baedf8d701 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-63f2a8f8ec"></a>
| Field | Shape |
|---|---|
| <a id="s-7635dc7bce"></a>`consumers` | ["stove0-server"] |
| <a id="s-0b8c0a3aae"></a>`name` | "STOVE0_RECIPES_PATH" |

## Governing policies

- <a id="pa-687c2c3919"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_RECIPES_PATH](../../../evidence/sources.md#src-c9dfc46efb) — `configuration-environment:STOVE0_RECIPES_PATH`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/109`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a9266907e07bd5c8b11591936696b86cf7e04385ff27bd313b005670d9e53ede -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_RECIPES_PATH"
}
```
