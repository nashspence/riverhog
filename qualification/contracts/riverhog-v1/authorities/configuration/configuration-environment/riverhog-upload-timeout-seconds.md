# RIVERHOG_UPLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-upload-timeout-seconds:390aa7ba63 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c741de7c16bc"></a>
| Field | Shape |
|---|---|
| <a id="s-3954c61aa8fb"></a>`consumers` | ["riverhog-client"] |
| <a id="s-ee62ea9044c7"></a>`name` | "RIVERHOG_UPLOAD_TIMEOUT_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_TIMEOUT_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_TIMEOUT_SECONDS](#s-c741de7c16bc) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-d3739b333a45"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-6215bdb1b9c4"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../../../evidence/sources.md#src-403ba6184b66) — `configuration-environment:RIVERHOG_UPLOAD_TIMEOUT_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/77`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f61ccdb9b3b414711281f34e686a61dcda01ae6af40bcf20387e1e616454592 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_UPLOAD_TIMEOUT_SECONDS"
}
```
