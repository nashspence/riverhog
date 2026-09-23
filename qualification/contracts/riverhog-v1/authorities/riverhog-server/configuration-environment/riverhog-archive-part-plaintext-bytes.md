# RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-part-plaintext-bytes:dc7f025598 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e0718e3ae1"></a>

| Field | Value |
|---|---|
| <a id="s-53c473dfe1"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-f1ca622372"></a>`default_expressions` | `["unset"]` |
| <a id="s-b157606d60"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES"` |
| <a id="s-2e49c887d8"></a>`input_shape` | `"environment-string"` |
| <a id="s-d3243722bc"></a>`name` | `"RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES"` |
| <a id="s-ff4993ce76"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](#s-e0718e3ae1) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-48b858c4e1"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-8eb46e3d52"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../../../evidence/sources/authorities.md#src-309bc75357) — [riverhog/src/riverhog\_core/collection\_plan.py::\_env\_bytes](../../../../../../riverhog/src/riverhog_core/collection_plan.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/collection\_plan.py](../../../../../../riverhog/src/riverhog_core/collection_plan.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/172`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
