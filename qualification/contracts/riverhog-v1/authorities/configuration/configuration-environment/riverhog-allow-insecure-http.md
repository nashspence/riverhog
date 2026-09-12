# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-allow-insecure-http:2e5af2ee82 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8f9926b885c8"></a>
| Field | Shape |
|---|---|
| <a id="s-9172f4f7f02c"></a>`consumers` | ["riverhog-client","riverhog-ftp-adapter","stove0-server"] |
| <a id="s-1fd2f143d53a"></a>`name` | "RIVERHOG_ALLOW_INSECURE_HTTP" |

## Governing policies

- <a id="pa-36241f92f098"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-50282d297a88) — `configuration-environment:RIVERHOG_ALLOW_INSECURE_HTTP`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/11`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7763338d7157144f83ae2912f603e8c04f8aa2105bfdf4dcd23eff2a37ae5e76 -->

```json
{
  "consumers": [
    "riverhog-client",
    "riverhog-ftp-adapter",
    "stove0-server"
  ],
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP"
}
```
