# A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-cli:a-riverhog-cli-upload-finalize-poll-seconds:01ba05f06d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c5d59cd44b"></a>

| Field | Value |
|---|---|
| <a id="s-4676feab29"></a>`consumers` | `["a-riverhog-cli"]` |
| <a id="s-5123e633a7"></a>`default_expressions` | `["unset"]` |
| <a id="s-8e21124594"></a>`id` | `"a-riverhog-cli:environment:A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS"` |
| <a id="s-22a7d99444"></a>`input_shape` | `"environment-string"` |
| <a id="s-0774de54c0"></a>`name` | `"A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS"` |
| <a id="s-020c7424d1"></a>`owner` | `"a-riverhog-cli"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS"; consumers=["a-riverhog-cli"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_CLI_UPLOAD_FINALIZE_POLL_SECONDS](#s-c5d59cd44b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f0007f52d9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-4d0bdcd7a8"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

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

- `/external_contract/configuration_environment/46`

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
