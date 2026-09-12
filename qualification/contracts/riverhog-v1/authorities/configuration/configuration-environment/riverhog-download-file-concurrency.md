# RIVERHOG_DOWNLOAD_FILE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-download-file-concurrency:b5ea9e29e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-1545d1acc2"></a>
| Field | Shape |
|---|---|
| <a id="s-5a43b64463"></a>`consumers` | ["riverhog-client"] |
| <a id="s-e838cafed4"></a>`name` | "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](#s-1545d1acc2) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-5c0905a3e6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-11b482a447"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../../../evidence/sources.md#src-2d3f1d8a66) — `configuration-environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/34`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0d67e64f48b91e32bc36bdd176b7a77838be2aff10ec078d2bf156096859a70 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"
}
```
