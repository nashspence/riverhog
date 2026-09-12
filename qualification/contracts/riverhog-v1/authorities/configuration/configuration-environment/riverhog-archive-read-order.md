# RIVERHOG_ARCHIVE_READ_ORDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-read-order:f5a96bc626 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d9dcf35fac"></a>
| Field | Shape |
|---|---|
| <a id="s-10a1d823b2"></a>`consumers` | ["riverhog-server"] |
| <a id="s-632be535e2"></a>`name` | "RIVERHOG_ARCHIVE_READ_ORDER" |

## Governing policies

- <a id="pa-c46c7392f0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_READ_ORDER](../../../evidence/sources.md#src-7716b4ded2) — `configuration-environment:RIVERHOG_ARCHIVE_READ_ORDER`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/16`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 819088c9b5bfff31b487542b88c12070cf0bc58e84f456c7d77f6e7506d93d29 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_READ_ORDER"
}
```
