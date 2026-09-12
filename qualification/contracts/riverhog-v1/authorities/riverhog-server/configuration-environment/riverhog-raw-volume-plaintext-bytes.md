# RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-raw-volume-plaintext-bytes:18af6755f6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-52addf4ec4"></a>
| Field | Shape |
|---|---|
| <a id="s-7aeb00f35c"></a>`classification` | "runtime" |
| <a id="s-d18a045679"></a>`consumers` | ["riverhog-server"] |
| <a id="s-31de2c48fd"></a>`disposition` | "contractual" |
| <a id="s-769fe4c6ca"></a>`id` | "riverhog-server:environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES" |
| <a id="s-a5c464e916"></a>`name` | "RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES" |
| <a id="s-ec108fde99"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](#s-52addf4ec4) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-283297b432"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-fbabedc498"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../../../evidence/sources.md#src-3fb717c8c3) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `_env_bytes(values, 'RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES', DEFAULT_RAW_VOLUME_PLAINTEXT_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/63`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00595f619db64e2cc2b55c21d2a6b0167f5070ef8fc83cb57a83f96032e1c4df -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES",
  "name": "RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES",
  "owner": "riverhog-server"
}
```
