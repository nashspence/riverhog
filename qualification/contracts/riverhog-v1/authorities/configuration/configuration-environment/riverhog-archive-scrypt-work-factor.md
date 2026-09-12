# RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-scrypt-work-factor:91ad8b229a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9de9805f44"></a>
| Field | Shape |
|---|---|
| <a id="s-f0e22869ab"></a>`consumers` | ["riverhog-server"] |
| <a id="s-81fae89078"></a>`name` | "RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR" |

## Governing policies

- <a id="pa-3023c33d5b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR](../../../evidence/sources.md#src-8e32132922) — `configuration-environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/17`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e21a62470c22ba1612d73051278a28e7945f77071967090a05648c307fe143a -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR"
}
```
