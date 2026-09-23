# STOVE0_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-http-timeout-seconds:77beb70893 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5d16c3676e"></a>

| Field | Value |
|---|---|
| <a id="s-a4f6afb693"></a>`consumers` | `["stove0-api-client"]` |
| <a id="s-3a22260cfc"></a>`default_expressions` | `["unset"]` |
| <a id="s-b53af91022"></a>`id` | `"stove0-api-client:environment:STOVE0_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-5fe728f6a8"></a>`input_shape` | `"environment-string"` |
| <a id="s-93dd6b430c"></a>`name` | `"STOVE0_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-36acc9190a"></a>`owner` | `"stove0-api-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_HTTP_TIMEOUT_SECONDS"; consumers=["stove0-api-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_HTTP_TIMEOUT_SECONDS](#s-5d16c3676e) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-142681f0d2"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-64ad6f6949"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_HTTP_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-a470f49fc7) — [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py::\_positive\_float\_env](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | [reference/stove0/packages/api-client/src/stove0\_api\_client/client.py](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/146`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1485f24a5835e12eeafbf2ae462c6b5ab5d5d062c770c4818bac20841501f602 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-api-client:environment:STOVE0_HTTP_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_HTTP_TIMEOUT_SECONDS",
  "owner": "stove0-api-client"
}
```

</details>
