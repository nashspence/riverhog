# RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-inflight-bytes:a14386c981 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-574fe9b8e4"></a>

| Field | Value |
|---|---|
| <a id="s-a7b2406b64"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-5396bdb526"></a>`default_expressions` | `["unset"]` |
| <a id="s-75451e7e9e"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES"` |
| <a id="s-47cc75791d"></a>`input_shape` | `"environment-string"` |
| <a id="s-600a648358"></a>`name` | `"RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES"` |
| <a id="s-16a516924a"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](#s-574fe9b8e4) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ba1a7c6297"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-63d00eb9bf"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../../../evidence/sources/authorities.md#src-cb29acf59b) — [riverhog/src/riverhog\_core/throughput.py::\_env\_bytes](../../../../../../riverhog/src/riverhog_core/throughput.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/throughput.py](../../../../../../riverhog/src/riverhog_core/throughput.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/210`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0cfc26586e996aee347209ec395d5a37b9c4ca31deb33c975ea38f387b81c7b -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES",
  "owner": "riverhog-server"
}
```

</details>
