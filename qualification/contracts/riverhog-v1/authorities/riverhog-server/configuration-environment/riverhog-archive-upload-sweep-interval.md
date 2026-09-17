# RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-upload-sweep-interval:5d275065b6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b86dc65d81"></a>

| Field | Value |
|---|---|
| <a id="s-1e81b89850"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-c2b6d2d02a"></a>`default_expressions` | `["'30s'"]` |
| <a id="s-657f4a2ce7"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL"` |
| <a id="s-af86639504"></a>`input_shape` | `"environment-string"` |
| <a id="s-08fd5560b5"></a>`name` | `"RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL"` |
| <a id="s-6f4b6e09b4"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](#s-b86dc65d81) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-9d249f6aee"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-4254ebecd7"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../../../evidence/sources.md#src-19a319fc4e) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL', '30s')` |

### Machine authority

- `/external_contract/configuration_environment/45`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cabf6027730b679a606539d31e701bcfd39e5adde70080ff6421b38fdd6b68b -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'30s'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL",
  "owner": "riverhog-server"
}
```

</details>
