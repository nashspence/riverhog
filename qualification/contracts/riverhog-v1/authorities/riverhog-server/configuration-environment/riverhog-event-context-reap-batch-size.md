# RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-context-reap-batch-size:4df7c116e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-53a88dfb96"></a>
| Field | Shape |
|---|---|
| <a id="s-cad8bbbd17"></a>`consumers` | ["riverhog-server"] |
| <a id="s-dadceebea3"></a>`default_expressions` | ["'100'"] |
| <a id="s-dbe47075d5"></a>`id` | "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE" |
| <a id="s-f6d00c3d7b"></a>`input_shape` | "environment-string" |
| <a id="s-c305899a87"></a>`name` | "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE" |
| <a id="s-02d8846ea8"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](#s-53a88dfb96) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-bf8a6f04bc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-bddc31216e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../../../evidence/sources.md#src-fef401b6a9) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE', '100')` |

### Machine authority

- `/external_contract/configuration_environment/58`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7cdef7895f4357311bec98c50f6d4092a7dd4220557df1902e613121a9feca77 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'100'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE",
  "owner": "riverhog-server"
}
```
