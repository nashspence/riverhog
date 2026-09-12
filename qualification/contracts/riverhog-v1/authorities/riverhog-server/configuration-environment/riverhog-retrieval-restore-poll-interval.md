# RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-restore-poll-interval:7836cabeec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-d42d9004a6"></a>
| Field | Shape |
|---|---|
| <a id="s-7c50e673ab"></a>`classification` | "runtime" |
| <a id="s-ef9d7442ff"></a>`consumers` | ["riverhog-server"] |
| <a id="s-7c6c832f01"></a>`disposition` | "contractual" |
| <a id="s-7b216e5267"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL" |
| <a id="s-c980de4ee4"></a>`name` | "RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL" |
| <a id="s-a9e838bc8b"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](#s-d42d9004a6) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4bef9c21fc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-9182f5b340"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../../../evidence/sources.md#src-4c5aea570f) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL', '5m')` |

### Machine authority

- `/external_contract/configuration_environment/79`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0da7a7a8b10b843c06de820b87d815f26db94aee9f0641e94f5eb53c17bcc34e -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL",
  "name": "RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL",
  "owner": "riverhog-server"
}
```
