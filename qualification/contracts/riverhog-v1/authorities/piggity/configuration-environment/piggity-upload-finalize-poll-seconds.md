# PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-upload-finalize-poll-seconds:fd4d6ff598 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e342da9aef"></a>

| Field | Value |
|---|---|
| <a id="s-1e916619c8"></a>`consumers` | `["piggity"]` |
| <a id="s-fa5f6c460e"></a>`default_expressions` | `["unset"]` |
| <a id="s-6907510a84"></a>`id` | `"piggity:environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"` |
| <a id="s-f2a6bcf8cf"></a>`input_shape` | `"environment-string"` |
| <a id="s-89688e57e4"></a>`name` | `"PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"` |
| <a id="s-b6d0b64384"></a>`owner` | `"piggity"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"; consumers=["piggity"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](#s-e342da9aef) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0a254a636a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-7ce3c6fd5c"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../../../evidence/sources/authorities.md#src-2057e6d1cc) — [reference/riverhog/applications/piggity/src/piggity/main.py::\_upload\_finalize\_poll\_seconds](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `piggity` | [reference/riverhog/applications/piggity/src/piggity/main.py](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py) | `os.getenv('PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS')` |

### Machine authority

- `/external_contract/configuration_environment/9`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4de716886e9c67cf949dab8f644e38d29cdf90eb5e325c9d2844aa3acf4afa89 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "piggity:environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS",
  "input_shape": "environment-string",
  "name": "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS",
  "owner": "piggity"
}
```

</details>
