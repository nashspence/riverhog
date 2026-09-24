# RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-ingress-max-inflight-bytes:f85def9b98 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4d9e9baa2d"></a>

| Field | Value |
|---|---|
| <a id="s-ac1870ac7e"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-dbeac2fd91"></a>`default_expressions` | `["unset"]` |
| <a id="s-88ec9d4401"></a>`id` | `"riverhog-server:environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"` |
| <a id="s-088ce46815"></a>`input_shape` | `"environment-string"` |
| <a id="s-4eb4f92e09"></a>`name` | `"RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"` |
| <a id="s-090693892d"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](#s-4d9e9baa2d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-05d8495ac6"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-4c202d6d73"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../../../evidence/sources/authorities.md#src-e1d4295958) — [riverhog/src/riverhog\_core/throughput.py::\_env\_bytes](../../../../../../riverhog/src/riverhog_core/throughput.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/throughput.py](../../../../../../riverhog/src/riverhog_core/throughput.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/194`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38de3b941aea75eff9fe63445bacb6faf3e976ca930df5aa30e018da6b000c7d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES",
  "owner": "riverhog-server"
}
```

</details>
