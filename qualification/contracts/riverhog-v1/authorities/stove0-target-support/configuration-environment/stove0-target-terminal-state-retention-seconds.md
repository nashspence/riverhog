# STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-target-support:stove0-target-terminal-state-retention-seconds:6946098c48 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-34051fda6d) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-ee5a428565"></a>
| Field | Shape |
|---|---|
| <a id="s-29173f7ae5"></a>`classification` | "runtime" |
| <a id="s-2023dd6f92"></a>`consumers` | ["stove0-target-support"] |
| <a id="s-868e028852"></a>`disposition` | "contractual" |
| <a id="s-0690b54cdc"></a>`id` | "stove0-target-support:environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS" |
| <a id="s-8563584654"></a>`name` | "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS" |
| <a id="s-3016559a93"></a>`owner` | "stove0-target-support" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"; consumers=["stove0-target-support"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](#s-ee5a428565) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-aa0b325a57"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-44ffd0b8b0"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-target-support:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../../../evidence/sources.md#src-a4a7b5aed4) — `reference/stove0/packages/target-support/src/stove0_target_support/configuration.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/29/names` |
| parser | `stove0-target-support` | `reference/stove0/packages/target-support/src/stove0_target_support/configuration.py` | `values.get(TARGET_TERMINAL_STATE_RETENTION_ENV, str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS))` |

### Machine authority

- `/external_contract/configuration_environment/126`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3de99d6da8f29e0f305716ee01afb2f8bf71fabe5b318816c092211ca58eb8f -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-target-support"
  ],
  "disposition": "contractual",
  "id": "stove0-target-support:environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS",
  "name": "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS",
  "owner": "stove0-target-support"
}
```
