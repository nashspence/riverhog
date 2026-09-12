# RIVERHOG_ARCHIVE_PASSPHRASES_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-passphrases-json:d6ef6602fe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-12ac638176"></a>
| Field | Shape |
|---|---|
| <a id="s-f82bf50d9b"></a>`consumers` | ["riverhog-server"] |
| <a id="s-b8a774d9b2"></a>`name` | "RIVERHOG_ARCHIVE_PASSPHRASES_JSON" |

## Governing policies

- <a id="pa-1b44c0d418"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON](../../../evidence/sources.md#src-c4962bbade) — `configuration-environment:RIVERHOG_ARCHIVE_PASSPHRASES_JSON`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/14`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6dd2d723f7d9b8116caaacc139d068b041b5883ac4ec5841cf06fb16a87d853 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_PASSPHRASES_JSON"
}
```
