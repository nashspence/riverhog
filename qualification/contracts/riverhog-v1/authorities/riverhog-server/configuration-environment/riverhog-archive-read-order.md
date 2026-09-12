# RIVERHOG_ARCHIVE_READ_ORDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-read-order:f500f59321 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](families/identity/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1c3c820c2c"></a>
| Field | Shape |
|---|---|
| <a id="s-9c19810799"></a>`classification` | "identity" |
| <a id="s-aca3251563"></a>`consumers` | ["riverhog-server"] |
| <a id="s-85387ce92e"></a>`disposition` | "contractual" |
| <a id="s-0463c6ecc3"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_READ_ORDER" |
| <a id="s-5945522bb9"></a>`name` | "RIVERHOG_ARCHIVE_READ_ORDER" |
| <a id="s-8beef43a66"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-cb4bedae44"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_READ_ORDER](../../../evidence/sources.md#src-b743528fa3) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/13/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `values.get('RIVERHOG_ARCHIVE_READ_ORDER', ','.join(names))` |

### Machine authority

- `/external_contract/configuration_environment/36`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 480ac7e71d22ea0238867e4a9e3dfe22feb54869e522aa0b8e2fa3ebe18213ae -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_READ_ORDER",
  "name": "RIVERHOG_ARCHIVE_READ_ORDER",
  "owner": "riverhog-server"
}
```
