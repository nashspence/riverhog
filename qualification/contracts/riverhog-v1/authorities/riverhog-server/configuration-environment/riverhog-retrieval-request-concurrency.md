# RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-request-concurrency:c23e64aec0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-658650862c"></a>

| Field | Value |
|---|---|
| <a id="s-e8619b82d7"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-676368be6a"></a>`default_expressions` | `["unset"]` |
| <a id="s-3dd36fbb03"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"` |
| <a id="s-7a1259948c"></a>`input_shape` | `"environment-string"` |
| <a id="s-dc1bc511db"></a>`name` | `"RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"` |
| <a id="s-6010d735e6"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](#s-658650862c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-008bf2249f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-a420718769"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

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

- `/external_contract/configuration_environment/83`

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
