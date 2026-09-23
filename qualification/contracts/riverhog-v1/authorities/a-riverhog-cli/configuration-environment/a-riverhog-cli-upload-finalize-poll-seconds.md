# A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-cli:a-riverhog-cli-upload-finalize-poll-seconds:b0dc3c9a8f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-960cd64e68"></a>

| Field | Value |
|---|---|
| <a id="s-194d4c0b99"></a>`consumers` | `["a-riverhog-cli"]` |
| <a id="s-64d0a96fda"></a>`default_expressions` | `["unset"]` |
| <a id="s-9b65d2f1fb"></a>`id` | `"a-riverhog-cli:environment:A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS"` |
| <a id="s-21b113f8ca"></a>`input_shape` | `"environment-string"` |
| <a id="s-fc06ab9bf9"></a>`name` | `"A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS"` |
| <a id="s-edda95a7cd"></a>`owner` | `"a-riverhog-cli"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS"; consumers=["a-riverhog-cli"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS](#s-960cd64e68) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1492e579ed"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-bafa97d5f2"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-cli:A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS](../../../evidence/sources/authorities.md#src-31393dad3f) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::\_upload\_finalize\_poll\_seconds](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-cli` | [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py) | `os.getenv('A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS')` |

### Machine authority

- `/external_contract/configuration_environment/100`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdd66d37301801af6b930e2baf06d28e61758786a6501905a2a0b86569ad4abb -->

```json
{
  "consumers": [
    "a-riverhog-cli"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-cli:environment:A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS",
  "owner": "a-riverhog-cli"
}
```

</details>
