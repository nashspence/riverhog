# RIVERHOG_DOWNLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-download-file-window:7fac8fbdfd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-6710649fb3a7"></a>
| Field | Shape |
|---|---|
| <a id="s-fe5e4264762a"></a>`consumers` | ["riverhog-client"] |
| <a id="s-3a8258723ca6"></a>`name` | "RIVERHOG_DOWNLOAD_FILE_WINDOW" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_WINDOW"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_WINDOW](#s-6710649fb3a7) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-f507487f40c7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-65cd2e349e3d"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW](../../../evidence/sources.md#src-a5ca05826c27) — `configuration-environment:RIVERHOG_DOWNLOAD_FILE_WINDOW`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/35`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1bd46150afff8bc344ac7422d801d5113f3dcd4491a17e802d84819f6af3e5ee -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_DOWNLOAD_FILE_WINDOW"
}
```
