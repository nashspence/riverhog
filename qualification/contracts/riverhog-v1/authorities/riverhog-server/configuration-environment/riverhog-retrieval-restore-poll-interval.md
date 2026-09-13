# RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-restore-poll-interval:34c07e5195 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-47fb606ae5"></a>
| Field | Shape |
|---|---|
| <a id="s-c114306576"></a>`consumers` | ["riverhog-server"] |
| <a id="s-f48a7336e8"></a>`default_expressions` | ["'5m'"] |
| <a id="s-8f3b8fe1e3"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL" |
| <a id="s-180063e963"></a>`input_shape` | "environment-string" |
| <a id="s-e3159c3eeb"></a>`name` | "RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL" |
| <a id="s-065409c482"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](#s-47fb606ae5) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-7c7eca9361"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-f2dc609ea1"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../../../evidence/sources.md#src-4c5aea570f) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL', '5m')` |

### Machine authority

- `/external_contract/configuration_environment/84`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62fe65df21bec409d33370d2eb0d269950ccd0149699dfd926c27c23521ff7a9 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'5m'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL",
  "owner": "riverhog-server"
}
```
