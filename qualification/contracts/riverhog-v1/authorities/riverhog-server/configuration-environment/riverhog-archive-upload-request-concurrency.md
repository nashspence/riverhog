# RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-upload-request-concurrency:d0f0ea566b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-25e3b58251"></a>

| Field | Value |
|---|---|
| <a id="s-71ff5053cc"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-161ddad828"></a>`default_expressions` | `["unset"]` |
| <a id="s-c822a85dc1"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"` |
| <a id="s-2cab81cbef"></a>`input_shape` | `"environment-string"` |
| <a id="s-3b82021d67"></a>`name` | `"RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"` |
| <a id="s-0b55c318c0"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](#s-25e3b58251) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ff47fb2286"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-2abedf676a"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../../../evidence/sources/authorities.md#src-b5ab0e168b) — [riverhog/src/riverhog\_core/throughput.py::\_env\_int](../../../../../../riverhog/src/riverhog_core/throughput.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/throughput.py](../../../../../../riverhog/src/riverhog_core/throughput.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/178`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8aec80624985f2648bca262f02dba64578612529f40e3737696b73bd24527414 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY",
  "owner": "riverhog-server"
}
```

</details>
