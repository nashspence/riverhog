# RIVERHOG_DOWNLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-file-window:4b80d5f871 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-eb8f28adc2"></a>

| Field | Value |
|---|---|
| <a id="s-f44dec2d22"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-73dce300f6"></a>`default_expressions` | `["''"]` |
| <a id="s-bf6317e26b"></a>`id` | `"riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_WINDOW"` |
| <a id="s-a9ad8d226b"></a>`input_shape` | `"environment-string"` |
| <a id="s-c875192905"></a>`name` | `"RIVERHOG_DOWNLOAD_FILE_WINDOW"` |
| <a id="s-9e801618d0"></a>`owner` | `"riverhog-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_WINDOW"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_WINDOW](#s-eb8f28adc2) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-61c1d9d721"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-1d0fae8a3b"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_WINDOW](../../../evidence/sources/authorities.md#src-4629108138) — [packages/riverhog-client/src/riverhog\_client/downloads.py::configured\_download\_window](../../../../../../packages/riverhog-client/src/riverhog_client/downloads.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/downloads.py](../../../../../../packages/riverhog-client/src/riverhog_client/downloads.py) | `environment.get('RIVERHOG_DOWNLOAD_FILE_WINDOW', '')` |

### Machine authority

- `/external_contract/configuration_environment/101`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 167f8e64419b3d5e01313381e02181e247300df4588834df1ce9dda4b6b013b9 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_WINDOW",
  "input_shape": "environment-string",
  "name": "RIVERHOG_DOWNLOAD_FILE_WINDOW",
  "owner": "riverhog-client"
}
```

</details>
