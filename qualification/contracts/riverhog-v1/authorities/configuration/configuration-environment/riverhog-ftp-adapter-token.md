# RIVERHOG_FTP_ADAPTER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-token:f9b3c800c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b86dc65d8121"></a>
| Field | Shape |
|---|---|
| <a id="s-1e81b89850f1"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-08fd5560b53f"></a>`name` | "RIVERHOG_FTP_ADAPTER_TOKEN" |

## Governing policies

- <a id="pa-efadd33ab9b0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN](../../../evidence/sources.md#src-0680cd1210d2) — `configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/45`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 738c8930bff67546a294f9b392414512b4711b5f54b2de19c5c6b67c78ba2018 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_TOKEN"
}
```
