# RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-request-concurrency:d3084ef46a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-637c0687c8"></a>

| Field | Value |
|---|---|
| <a id="s-d510a34fe7"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-bcd1eb8d6b"></a>`default_expressions` | `["unset"]` |
| <a id="s-094063ee21"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"` |
| <a id="s-c99cd0a627"></a>`input_shape` | `"environment-string"` |
| <a id="s-91f311aaa8"></a>`name` | `"RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"` |
| <a id="s-0a7e0dfdeb"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](#s-637c0687c8) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5d69148c7f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-1c8987079a"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../../../evidence/sources/authorities.md#src-a586c2cf3e) — [riverhog/src/riverhog\_core/throughput.py::\_env\_int](../../../../../../riverhog/src/riverhog_core/throughput.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/throughput.py](../../../../../../riverhog/src/riverhog_core/throughput.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/216`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc205fd196332c087c59a20fd5a1b233cb8af7fdfc47e7fe717148d768e3c060 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY",
  "owner": "riverhog-server"
}
```

</details>
