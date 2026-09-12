# RIVERHOG_PACK_FILES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-pack-files:0f490c209d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ef146f919d"></a>
| Field | Shape |
|---|---|
| <a id="s-93a23b729e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-4e7ddbc9fe"></a>`name` | "RIVERHOG_PACK_FILES" |

## Governing policies

- <a id="pa-c3ef6abe8c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_PACK_FILES](../../../evidence/sources.md#src-193a002a35) — `configuration-environment:RIVERHOG_PACK_FILES`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/52`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3485458d867968153cf9b7ed1a8a3a1550088ea7a31ea8cf1aba17d062072ded -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_PACK_FILES"
}
```
