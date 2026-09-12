# RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-http-timeout-seconds:dd2944347f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-ee975e99ad"></a>
| Field | Shape |
|---|---|
| <a id="s-ca55d6dc58"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-1a8ef76883"></a>`name` | "RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](#s-ee975e99ad) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-1ce5a598d3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2676843eb7"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../../../evidence/sources.md#src-f4c02d915f) — `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/44`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb922dba02e71fa21438f668b84f633d312b79a59d69bd0d9cd4a118ad0c06d0 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS"
}
```
