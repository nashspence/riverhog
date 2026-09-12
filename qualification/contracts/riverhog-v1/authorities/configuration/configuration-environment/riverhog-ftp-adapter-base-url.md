# RIVERHOG_FTP_ADAPTER_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-base-url:a30fa80299 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b200cddc47"></a>
| Field | Shape |
|---|---|
| <a id="s-a64ea888cd"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-5b43242159"></a>`name` | "RIVERHOG_FTP_ADAPTER_BASE_URL" |

## Governing policies

- <a id="pa-237a09520c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL](../../../evidence/sources.md#src-b54fad905c) — `configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/41`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1e45cc4e25d2d36930ab7c3431031589fff494ddbd0e9ac7ad3ff6d6baa0647 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_BASE_URL"
}
```
