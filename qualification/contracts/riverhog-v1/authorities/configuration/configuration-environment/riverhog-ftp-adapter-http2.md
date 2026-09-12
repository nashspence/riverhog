# RIVERHOG_FTP_ADAPTER_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-http2:e133274d86 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c80b06088081"></a>
| Field | Shape |
|---|---|
| <a id="s-fe8922d85984"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-e9729640afc1"></a>`name` | "RIVERHOG_FTP_ADAPTER_HTTP2" |

## Governing policies

- <a id="pa-1b6b0d0ac593"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2](../../../evidence/sources.md#src-4a3517eac73c) — `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/43`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cea2e99ea406e328870b3277e7759b8286ef84a8f5883ac5f200c6e1a09c3bd6 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_HTTP2"
}
```
