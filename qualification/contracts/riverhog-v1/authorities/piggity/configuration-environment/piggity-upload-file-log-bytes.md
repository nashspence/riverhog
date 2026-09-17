# PIGGITY_UPLOAD_FILE_LOG_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-upload-file-log-bytes:c44ce6204d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1966a8d0ec"></a>

| Field | Value |
|---|---|
| <a id="s-9a2311e6cd"></a>`consumers` | `["piggity"]` |
| <a id="s-6b12b9694a"></a>`default_expressions` | `["unset"]` |
| <a id="s-cf3af0a6fd"></a>`id` | `"piggity:environment:PIGGITY_UPLOAD_FILE_LOG_BYTES"` |
| <a id="s-0f9cc1851c"></a>`input_shape` | `"environment-string"` |
| <a id="s-4341315209"></a>`name` | `"PIGGITY_UPLOAD_FILE_LOG_BYTES"` |
| <a id="s-4ea39658ad"></a>`owner` | `"piggity"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="PIGGITY_UPLOAD_FILE_LOG_BYTES"; consumers=["piggity"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [PIGGITY_UPLOAD_FILE_LOG_BYTES](#s-1966a8d0ec) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-3ddfdeca2f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-9f0fd89a14"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:piggity:PIGGITY_UPLOAD_FILE_LOG_BYTES](../../../evidence/sources/authorities.md#src-283d9ea8ad) — [reference/riverhog/applications/piggity/src/piggity/main.py::\_upload\_file\_log\_bytes](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `piggity` | [reference/riverhog/applications/piggity/src/piggity/main.py](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py) | `os.getenv('PIGGITY_UPLOAD_FILE_LOG_BYTES')` |

### Machine authority

- `/external_contract/configuration_environment/8`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b80bc6ba9997482476f4898e98c8add2b2e1f2006d3fbdaf6649e42de2c90881 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "piggity:environment:PIGGITY_UPLOAD_FILE_LOG_BYTES",
  "input_shape": "environment-string",
  "name": "PIGGITY_UPLOAD_FILE_LOG_BYTES",
  "owner": "piggity"
}
```

</details>
