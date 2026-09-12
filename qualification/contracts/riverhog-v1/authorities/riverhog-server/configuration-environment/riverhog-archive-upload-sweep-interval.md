# RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-upload-sweep-interval:1e89b8e285 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c26bd33aa8"></a>
| Field | Shape |
|---|---|
| <a id="s-4d5140ab4a"></a>`classification` | "runtime" |
| <a id="s-cbfe80a062"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8336f9a674"></a>`disposition` | "contractual" |
| <a id="s-a3f3fe3044"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL" |
| <a id="s-501ec97f37"></a>`name` | "RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL" |
| <a id="s-06c79d612b"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](#s-c26bd33aa8) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-7244fab20f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-5ec1b8af42"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../../../evidence/sources.md#src-19a319fc4e) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL', '30s')` |

### Machine authority

- `/external_contract/configuration_environment/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73487c127e980da66b3ff4a1b4f3b073c8dffe3816a5b16572dab98b224aaf5a -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL",
  "name": "RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL",
  "owner": "riverhog-server"
}
```
