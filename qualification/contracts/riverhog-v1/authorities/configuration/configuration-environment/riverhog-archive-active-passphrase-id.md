# RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-active-passphrase-id:1718e7ca5b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b6148dfd814d"></a>
| Field | Shape |
|---|---|
| <a id="s-b49faf4a7f3b"></a>`consumers` | ["riverhog-server"] |
| <a id="s-a2dcbb91e667"></a>`name` | "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID" |

## Governing policies

- <a id="pa-52402111100c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID](../../../evidence/sources.md#src-103aaaade7fe) — `configuration-environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/12`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9476252509abf5f6bbc2eda8f2b4c22ffd5b7b91ba899374e0e916b84d08a75d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID"
}
```
