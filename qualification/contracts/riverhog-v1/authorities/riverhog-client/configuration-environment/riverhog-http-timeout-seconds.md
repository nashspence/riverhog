# RIVERHOG_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-http-timeout-seconds:e2c1d7c9a9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8f59324207"></a>

| Field | Value |
|---|---|
| <a id="s-5ce476d592"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-3b4a852639"></a>`default_expressions` | `["unset"]` |
| <a id="s-8697d6dfab"></a>`id` | `"riverhog-client:environment:RIVERHOG_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-7e4f4054fb"></a>`input_shape` | `"environment-string"` |
| <a id="s-514286516a"></a>`name` | `"RIVERHOG_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-2e2d9517c4"></a>`owner` | `"riverhog-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_HTTP_TIMEOUT_SECONDS"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_HTTP_TIMEOUT_SECONDS](#s-8f59324207) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f94de76c91"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-8908897b59"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_HTTP_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-908bba2b9b) — [packages/riverhog-client/src/riverhog\_client/client.py::\_timeout\_seconds](../../../../../../packages/riverhog-client/src/riverhog_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/client.py](../../../../../../packages/riverhog-client/src/riverhog_client/client.py) | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/161`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c65ef3c9f593c1d8d7d30253ebd4256b1e28c1d682cec4fe03a7bf447c5399be -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_HTTP_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_HTTP_TIMEOUT_SECONDS",
  "owner": "riverhog-client"
}
```

</details>
