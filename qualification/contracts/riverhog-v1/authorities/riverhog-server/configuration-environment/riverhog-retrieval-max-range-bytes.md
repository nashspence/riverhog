# RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-range-bytes:d2fb9ddd7b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ca48950b02"></a>

| Field | Value |
|---|---|
| <a id="s-7fa6d62383"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-05645c32dd"></a>`default_expressions` | `["unset"]` |
| <a id="s-95fbc5f58a"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES"` |
| <a id="s-a43ef310a3"></a>`input_shape` | `"environment-string"` |
| <a id="s-ec1467afc9"></a>`name` | `"RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES"` |
| <a id="s-bb162cee71"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](#s-ca48950b02) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-52e86b9152"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-b4557dbc17"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../../../evidence/sources/authorities.md#src-1e662f4cb2) — [riverhog/src/riverhog\_core/pack\_retrieval.py::\_scoped\_env\_value](../../../../../../riverhog/src/riverhog_core/pack_retrieval.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/pack\_retrieval.py](../../../../../../riverhog/src/riverhog_core/pack_retrieval.py) | `values.get(global_name)` |

### Machine authority

- `/external_contract/configuration_environment/211`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81cb306ced57919ff107782ba1a6a1b78572e329aa77645d4f82a856c83e2d6c -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES",
  "owner": "riverhog-server"
}
```

</details>
