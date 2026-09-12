# RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-archive-upload-request-concurrency:c572ae009b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-dc3b9e297a"></a>
| Field | Shape |
|---|---|
| <a id="s-2cfcf2ca71"></a>`consumers` | ["riverhog-server"] |
| <a id="s-3d0c74b453"></a>`name` | "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](#s-dc3b9e297a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-a48c78e80b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-e90376fa26"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../../../evidence/sources.md#src-7abda774e4) — `configuration-environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/19`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77bb88aabe208ff97dbef7a2a4fa6dbc45ebec0766a68bf285d0a2cf1000867f -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"
}
```
