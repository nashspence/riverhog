# RIVERHOG_RETRIEVAL_DEFAULT_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-default-lease:fd2fae3766 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e17f28d8d8"></a>

| Field | Value |
|---|---|
| <a id="s-a8f98984dc"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-1a8627f417"></a>`default_expressions` | `["'24h'"]` |
| <a id="s-e1a39d2288"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE"` |
| <a id="s-49efae42e7"></a>`input_shape` | `"environment-string"` |
| <a id="s-72292b5725"></a>`name` | `"RIVERHOG_RETRIEVAL_DEFAULT_LEASE"` |
| <a id="s-5a79a41389"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_DEFAULT_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_DEFAULT_LEASE](#s-e17f28d8d8) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-621de8ff05"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-f8c998e44f"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../../../evidence/sources/authorities.md#src-4fd9a92a9d) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_RETRIEVAL_DEFAULT_LEASE', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/74`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebee515585743ad5f61b80538ce92ce2c35e7f3db5cd1d8f03d4080d3283a367 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'24h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_DEFAULT_LEASE",
  "owner": "riverhog-server"
}
```

</details>
