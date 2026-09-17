# PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-upload-finalize-timeout-seconds:9dfbb3ac4b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b7a4f6f148"></a>

| Field | Value |
|---|---|
| <a id="s-7590734788"></a>`consumers` | `["piggity"]` |
| <a id="s-d8ebd849d8"></a>`default_expressions` | `["unset"]` |
| <a id="s-a600f51a3c"></a>`id` | `"piggity:environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS"` |
| <a id="s-81d1d3813e"></a>`input_shape` | `"environment-string"` |
| <a id="s-727a81b3ac"></a>`name` | `"PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS"` |
| <a id="s-10a1945249"></a>`owner` | `"piggity"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS"; consumers=["piggity"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](#s-b7a4f6f148) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-83f0b3ae69"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-56e7e300e5"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../../../evidence/sources.md#src-40c863880d) — [reference/riverhog/applications/piggity/src/piggity/main.py::\_upload\_finalize\_timeout\_seconds](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `piggity` | [reference/riverhog/applications/piggity/src/piggity/main.py](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py) | `os.getenv('PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS')` |

### Machine authority

- `/external_contract/configuration_environment/10`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c4f5569d2f1b05e173bfc8eb4a04c5ca03b530c5c9a3af4cd89174590464822e -->

```json
{
  "consumers": [
    "piggity"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "piggity:environment:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS",
  "owner": "piggity"
}
```

</details>
