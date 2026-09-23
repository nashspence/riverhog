# RIVERHOG_RETRIEVAL_PENDING_TIMEOUT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-pending-timeout:1ad04a8eec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-986a5dc23a"></a>

| Field | Value |
|---|---|
| <a id="s-72c970d79e"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-d2217a5807"></a>`default_expressions` | `["'72h'"]` |
| <a id="s-e7469eabe2"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT"` |
| <a id="s-f0db4b7c89"></a>`input_shape` | `"environment-string"` |
| <a id="s-adf35c1f47"></a>`name` | `"RIVERHOG_RETRIEVAL_PENDING_TIMEOUT"` |
| <a id="s-14d34e06dd"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_PENDING_TIMEOUT"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](#s-986a5dc23a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c8d74f933c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-b27d40cb72"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../../../evidence/sources/authorities.md#src-01bd40bbb3) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_RETRIEVAL_PENDING_TIMEOUT', '72h')` |

### Machine authority

- `/external_contract/configuration_environment/213`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13d6f5c3c60e7a02158c9148573e0ee71f192f86057f8190ae9828a4b5f30236 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'72h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_PENDING_TIMEOUT",
  "owner": "riverhog-server"
}
```

</details>
