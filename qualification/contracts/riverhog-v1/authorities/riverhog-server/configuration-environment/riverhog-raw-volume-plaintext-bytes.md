# RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-raw-volume-plaintext-bytes:e926f8ea95 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-3bf78de3fa"></a>
| Field | Shape |
|---|---|
| <a id="s-9364d1a9f1"></a>`consumers` | ["riverhog-server"] |
| <a id="s-92b6a91362"></a>`default_expressions` | ["unset"] |
| <a id="s-6a8524b81a"></a>`id` | "riverhog-server:environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES" |
| <a id="s-594729b07d"></a>`input_shape` | "environment-string" |
| <a id="s-990d2b0cff"></a>`name` | "RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES" |
| <a id="s-5b3245d215"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](#s-3bf78de3fa) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-fe8fc8eb99"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-aded04b126"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../../../evidence/sources.md#src-3fb717c8c3) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/68`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ecd7a5add6f3383735e5976362e68479aca3499d02419c05db36e7c1f4fac766 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES",
  "owner": "riverhog-server"
}
```
