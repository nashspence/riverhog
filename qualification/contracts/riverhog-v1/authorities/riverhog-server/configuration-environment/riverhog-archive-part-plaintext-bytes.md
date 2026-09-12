# RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-part-plaintext-bytes:53b3223595 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-f77147b1c3"></a>
| Field | Shape |
|---|---|
| <a id="s-ed608794c0"></a>`classification` | "runtime" |
| <a id="s-4504ac9a24"></a>`consumers` | ["riverhog-server"] |
| <a id="s-37d634082c"></a>`disposition` | "contractual" |
| <a id="s-d6c9861ab7"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES" |
| <a id="s-8af67c498b"></a>`name` | "RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES" |
| <a id="s-e1481d42fc"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](#s-f77147b1c3) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-756e42e6be"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-8dedddc75c"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../../../evidence/sources.md#src-309bc75357) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `_env_bytes(values, 'RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES', DEFAULT_PART_PLAINTEXT_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/33`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f53a5491a6c197a65004c9ae698777985f6c5ddafb54ca7a130861730a268464 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES",
  "name": "RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES",
  "owner": "riverhog-server"
}
```
