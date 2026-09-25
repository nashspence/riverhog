# A_RIVERHOG_CLI_LOCAL_DATABASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-cli:a-riverhog-cli-local-database:1271204c2b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ba8bf00da0"></a>

| Field | Value |
|---|---|
| <a id="s-fefed5a14e"></a>`consumers` | `["a-riverhog-cli"]` |
| <a id="s-9f9519b503"></a>`default_expressions` | `["''"]` |
| <a id="s-6e9078eeef"></a>`id` | `"a-riverhog-cli:environment:A_RIVERHOG_CLI_LOCAL_DATABASE"` |
| <a id="s-b017fa7a37"></a>`input_shape` | `"environment-string"` |
| <a id="s-5c224fe460"></a>`name` | `"A_RIVERHOG_CLI_LOCAL_DATABASE"` |
| <a id="s-316a058fc1"></a>`owner` | `"a-riverhog-cli"` |

## Governing policies

- <a id="pa-3adfcddcd7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-cli:A_RIVERHOG_CLI_LOCAL_DATABASE](../../../evidence/sources/authorities.md#src-73f7bef1ce) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py::\_database](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-cli` | [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py) | `os.getenv('A_RIVERHOG_CLI_LOCAL_DATABASE', '')` |

### Machine authority

- `/external_contract/configuration_environment/42`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5d890ff7d93906979e8d2d0845c2b4581fa11285e40238f41b8279a86802d06 -->

```json
{
  "consumers": [
    "a-riverhog-cli"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-cli:environment:A_RIVERHOG_CLI_LOCAL_DATABASE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_CLI_LOCAL_DATABASE",
  "owner": "a-riverhog-cli"
}
```

</details>
