# RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-allow-insecure-http:0f02bf74a5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c26bd33aa847"></a>
| Field | Shape |
|---|---|
| <a id="s-cbfe80a0622f"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-501ec97f37fc"></a>`name` | "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP" |

## Governing policies

- <a id="pa-863feda00e41"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-3bd2b96688c7) — `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fbc27c92dfb581e67fb969bb3f829f36479487be2343d8974725c0a3b0000706 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP"
}
```
