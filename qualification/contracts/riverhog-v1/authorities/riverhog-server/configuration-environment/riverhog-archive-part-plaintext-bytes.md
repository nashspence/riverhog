# RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-part-plaintext-bytes:70a63ea0e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c6af80cc8d"></a>
| Field | Shape |
|---|---|
| <a id="s-dc771f99e6"></a>`consumers` | ["riverhog-server"] |
| <a id="s-119b2b8dfe"></a>`default_expressions` | ["unset"] |
| <a id="s-a725fad84c"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES" |
| <a id="s-9285c08ab7"></a>`input_shape` | "environment-string" |
| <a id="s-3869830d71"></a>`name` | "RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES" |
| <a id="s-ff15d2537d"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](#s-c6af80cc8d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-b9cea8a4c9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-0bbbc96ac3"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../../../evidence/sources.md#src-309bc75357) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/38`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 604fea4ae26f8fae498dda67c2ff6c399d7727ad0daadac56e664500380eb906 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES",
  "owner": "riverhog-server"
}
```
