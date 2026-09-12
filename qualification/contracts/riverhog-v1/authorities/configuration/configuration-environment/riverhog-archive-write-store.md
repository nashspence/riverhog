# RIVERHOG_ARCHIVE_WRITE_STORE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-write-store:7b36be187c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f3c5c446af"></a>
| Field | Shape |
|---|---|
| <a id="s-e5f41d64e9"></a>`consumers` | ["riverhog-server"] |
| <a id="s-034b439c45"></a>`name` | "RIVERHOG_ARCHIVE_WRITE_STORE" |

## Governing policies

- <a id="pa-e465ce1cd1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE](../../../evidence/sources.md#src-755098156c) — `configuration-environment:RIVERHOG_ARCHIVE_WRITE_STORE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/22`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b68d7f8ae5f695d170f32fb6584bc6ba712cba7fc84929f4f721d8108bdb6b36 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_WRITE_STORE"
}
```
