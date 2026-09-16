# RIVERHOG_PACK_MEMBER_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-member-bytes:4c22a1ebc8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5c633c982d"></a>

| Field | Value |
|---|---|
| <a id="s-093b7de81f"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-cb2ce64f5f"></a>`default_expressions` | `["unset"]` |
| <a id="s-e6605379f6"></a>`id` | `"riverhog-server:environment:RIVERHOG_PACK_MEMBER_BYTES"` |
| <a id="s-1160de2df9"></a>`input_shape` | `"environment-string"` |
| <a id="s-340ef8e53f"></a>`name` | `"RIVERHOG_PACK_MEMBER_BYTES"` |
| <a id="s-51770eadf0"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_PACK_MEMBER_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_PACK_MEMBER_BYTES](#s-5c633c982d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-8b247ce9c0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2c589300d0"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_PACK_MEMBER_BYTES](../../../evidence/sources.md#src-f8aff5063d) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/65`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68a1621d532485dd86ce8d718cecc3540f91674be2b13b36ebefce9a906986b6 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_PACK_MEMBER_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_PACK_MEMBER_BYTES",
  "owner": "riverhog-server"
}
```

</details>
