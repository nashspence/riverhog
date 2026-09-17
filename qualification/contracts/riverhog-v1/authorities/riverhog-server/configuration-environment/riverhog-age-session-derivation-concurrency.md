# RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-age-session-derivation-concurrency:37eb593060 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1c3c820c2c"></a>

| Field | Value |
|---|---|
| <a id="s-aca3251563"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-210c2b46d5"></a>`default_expressions` | `["unset"]` |
| <a id="s-0463c6ecc3"></a>`id` | `"riverhog-server:environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY"` |
| <a id="s-36a54fae1a"></a>`input_shape` | `"environment-string"` |
| <a id="s-5945522bb9"></a>`name` | `"RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY"` |
| <a id="s-8beef43a66"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](#s-1c3c820c2c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-c7c29696e7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-8797688ab9"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../../../evidence/sources.md#src-b3f6d318c3) — [riverhog/src/riverhog\_core/throughput.py::\_env\_int](../../../../../../riverhog/src/riverhog_core/throughput.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/throughput.py](../../../../../../riverhog/src/riverhog_core/throughput.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/36`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23f86a0918d7951549db7fd04113632b97102ce77961f521149d672161d72441 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY",
  "owner": "riverhog-server"
}
```

</details>
