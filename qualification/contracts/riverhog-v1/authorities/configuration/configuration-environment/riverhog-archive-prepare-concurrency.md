# RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-prepare-concurrency:2f282ebc50 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-1ebfad79c3"></a>
| Field | Shape |
|---|---|
| <a id="s-42ed9823cb"></a>`consumers` | ["riverhog-server"] |
| <a id="s-def2479a70"></a>`name` | "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](#s-1ebfad79c3) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-f331af924a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-fe2a818a26"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../../../evidence/sources.md#src-1510b0ce21) — `configuration-environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/15`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f000b8abea6ef3c278a5e7fc557fdaeb93469ea3a513ca1167fdb6d108b197c8 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"
}
```
