# RIVERHOG_RETRIEVAL_MAX_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-lease:70c7461a92 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c741de7c16"></a>

| Field | Value |
|---|---|
| <a id="s-3954c61aa8"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-56d7952e75"></a>`default_expressions` | `["'7d'"]` |
| <a id="s-e3fea57c53"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_LEASE"` |
| <a id="s-fc18258489"></a>`input_shape` | `"environment-string"` |
| <a id="s-ee62ea9044"></a>`name` | `"RIVERHOG_RETRIEVAL_MAX_LEASE"` |
| <a id="s-175cffa6ac"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_LEASE](#s-c741de7c16) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-7599ab4723"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-ae775cddf6"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_LEASE](../../../evidence/sources/authorities.md#src-c44aeb9ce3) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_RETRIEVAL_MAX_LEASE', '7d')` |

### Machine authority

- `/external_contract/configuration_environment/77`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a45acc411cdff61c5c1cff4b38eb44da24cd8bc4b0bea90baa7a16a109b0d5ba -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'7d'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_LEASE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_MAX_LEASE",
  "owner": "riverhog-server"
}
```

</details>
