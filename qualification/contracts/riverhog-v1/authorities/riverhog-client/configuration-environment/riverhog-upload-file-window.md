# RIVERHOG_UPLOAD_FILE_WINDOW

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-upload-file-window:ad4577c8e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8af4063a2b"></a>

| Field | Value |
|---|---|
| <a id="s-780fea1d0c"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-81227f6e06"></a>`default_expressions` | `["''"]` |
| <a id="s-2760bd28e2"></a>`id` | `"riverhog-client:environment:RIVERHOG_UPLOAD_FILE_WINDOW"` |
| <a id="s-4a2f875513"></a>`input_shape` | `"environment-string"` |
| <a id="s-9feb87a0c4"></a>`name` | `"RIVERHOG_UPLOAD_FILE_WINDOW"` |
| <a id="s-095861fb48"></a>`owner` | `"riverhog-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_UPLOAD_FILE_WINDOW"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_UPLOAD_FILE_WINDOW](#s-8af4063a2b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ab1fcdab1c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-2c46db4e07"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_WINDOW](../../../evidence/sources/authorities.md#src-93e4082935) — [packages/riverhog-client/src/riverhog\_client/uploads.py::configured\_upload\_window](../../../../../../packages/riverhog-client/src/riverhog_client/uploads.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/uploads.py](../../../../../../packages/riverhog-client/src/riverhog_client/uploads.py) | `environment.get('RIVERHOG_UPLOAD_FILE_WINDOW', '')` |

### Machine authority

- `/external_contract/configuration_environment/108`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc22d3a4233a02d4708af7eefab7164674a5f35e44dadc157673270ff03f8e52 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_UPLOAD_FILE_WINDOW",
  "input_shape": "environment-string",
  "name": "RIVERHOG_UPLOAD_FILE_WINDOW",
  "owner": "riverhog-client"
}
```

</details>
