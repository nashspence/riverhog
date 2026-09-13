# RIVERHOG_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-http-timeout-seconds:88c9199de7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-7ff1624ad7) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-dc3b9e297a"></a>
| Field | Shape |
|---|---|
| <a id="s-2cfcf2ca71"></a>`consumers` | ["riverhog-client"] |
| <a id="s-221130ca92"></a>`default_expressions` | ["unset"] |
| <a id="s-57bcabee8d"></a>`id` | "riverhog-client:environment:RIVERHOG_HTTP_TIMEOUT_SECONDS" |
| <a id="s-fd0988394a"></a>`input_shape` | "environment-string" |
| <a id="s-3d0c74b453"></a>`name` | "RIVERHOG_HTTP_TIMEOUT_SECONDS" |
| <a id="s-90ff047361"></a>`owner` | "riverhog-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_HTTP_TIMEOUT_SECONDS"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_HTTP_TIMEOUT_SECONDS](#s-dc3b9e297a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-060be294c9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-fe9e95331c"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_HTTP_TIMEOUT_SECONDS](../../../evidence/sources.md#src-908bba2b9b) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv(env_name)` |

### Machine authority

- `/external_contract/configuration_environment/19`

### Exact owned JSON

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
