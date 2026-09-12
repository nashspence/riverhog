# STOVE0_DATABASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-database-url:547493b77d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-284bbb4914bf"></a>
| Field | Shape |
|---|---|
| <a id="s-c9aa0936f8cf"></a>`consumers` | ["stove0-server"] |
| <a id="s-9f2399bb48a9"></a>`name` | "STOVE0_DATABASE_URL" |

## Governing policies

- <a id="pa-55fdce4f5ba7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_DATABASE_URL](../../../evidence/sources.md#src-f3edb540b3c0) — `configuration-environment:STOVE0_DATABASE_URL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/86`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7128015da502442a3f9e39549ec54d7f092bc0551f48d8ab6876be4f964b652 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_DATABASE_URL"
}
```
