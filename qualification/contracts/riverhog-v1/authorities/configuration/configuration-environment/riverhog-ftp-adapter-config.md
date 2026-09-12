# RIVERHOG_FTP_ADAPTER_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-config:343b1f81d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ba8bf00da01d"></a>
| Field | Shape |
|---|---|
| <a id="s-fefed5a14e6a"></a>`consumers` | ["riverhog-ftp-adapter"] |
| <a id="s-5c224fe46039"></a>`name` | "RIVERHOG_FTP_ADAPTER_CONFIG" |

## Governing policies

- <a id="pa-42c2f6b15d62"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG](../../../evidence/sources.md#src-b14eb675d528) — `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/42`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: abe11f8b9b5115d2defebd9ae29cdeee8aa928191d7f781a5052f6d7957e04ea -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_CONFIG"
}
```
