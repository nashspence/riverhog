# RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-ingress-max-inflight-bytes:63d3580c10 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-a9033edf7a"></a>
| Field | Shape |
|---|---|
| <a id="s-7c2ffa6bcc"></a>`consumers` | ["riverhog-server"] |
| <a id="s-e820886cb1"></a>`default_expressions` | ["unset"] |
| <a id="s-5b8d93af24"></a>`id` | "riverhog-server:environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES" |
| <a id="s-524af15492"></a>`input_shape` | "environment-string" |
| <a id="s-7e4a9547db"></a>`name` | "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES" |
| <a id="s-c53e7cc506"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](#s-a9033edf7a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-572b74bf29"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-bd02222cd7"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../../../evidence/sources.md#src-e1d4295958) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/61`

### Exact owned JSON

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
