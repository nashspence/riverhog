# RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-estimated-latency:77825a3682 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-91af6fdd5c"></a>
| Field | Shape |
|---|---|
| <a id="s-30cf8592e6"></a>`classification` | "runtime" |
| <a id="s-b3f49ca63f"></a>`consumers` | ["riverhog-server"] |
| <a id="s-d067a325f8"></a>`disposition` | "contractual" |
| <a id="s-202000a564"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY" |
| <a id="s-814b85f667"></a>`name` | "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY" |
| <a id="s-4cd44f5a55"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-650eb275ff"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY](../../../evidence/sources.md#src-63c6f042b8) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY', '48h')` |

### Machine authority

- `/external_contract/configuration_environment/70`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab7860401a2c0a0d14d9bf42e7807b4cd6bc9f8fca1f71ee6a63c4f1ea80530f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY",
  "name": "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY",
  "owner": "riverhog-server"
}
```
