# RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-collection-upload-custody-lease:f1a1751da0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-aa4f15d37f"></a>

| Field | Value |
|---|---|
| <a id="s-c2a9438d60"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-d1035c754d"></a>`default_expressions` | `["'1h'"]` |
| <a id="s-0fc161ae60"></a>`id` | `"riverhog-server:environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"` |
| <a id="s-cc2b365de6"></a>`input_shape` | `"environment-string"` |
| <a id="s-c35c4f35c0"></a>`name` | `"RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"` |
| <a id="s-213db7b774"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](#s-aa4f15d37f) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-099d477166"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-67624fff83"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../../../evidence/sources/authorities.md#src-4cfc3a8358) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE', '1h')` |

### Machine authority

- `/external_contract/configuration_environment/190`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77d803267c8b64aaeb043d8471cdef294f499101f9b04d4dac2fd630ed25d49f -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'1h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE",
  "owner": "riverhog-server"
}
```

</details>
