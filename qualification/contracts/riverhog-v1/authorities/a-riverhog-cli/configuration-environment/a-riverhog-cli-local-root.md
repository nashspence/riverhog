# A_RIVERHOG_CLI_LOCAL_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-cli:a-riverhog-cli-local-root:4b51727d94 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5b5ad305a7"></a>

| Field | Value |
|---|---|
| <a id="s-d3dd57dbb2"></a>`consumers` | `["a-riverhog-cli"]` |
| <a id="s-c889318b3c"></a>`default_expressions` | `["''"]` |
| <a id="s-6178250fdc"></a>`id` | `"a-riverhog-cli:environment:A_RIVERHOG_CLI_LOCAL_ROOT"` |
| <a id="s-fe5e59c7f3"></a>`input_shape` | `"environment-string"` |
| <a id="s-cf768fb7d8"></a>`name` | `"A_RIVERHOG_CLI_LOCAL_ROOT"` |
| <a id="s-87339d9e18"></a>`owner` | `"a-riverhog-cli"` |

## Governing policies

- <a id="pa-04cd258ab9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-cli:A_RIVERHOG_CLI_LOCAL_ROOT](../../../evidence/sources/authorities.md#src-caeeea031d) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py::\_target](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-cli` | [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/local.py](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/local.py) | `os.getenv('A_RIVERHOG_CLI_LOCAL_ROOT', '')` |

### Machine authority

- `/external_contract/configuration_environment/97`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ac03c3f176a80fce13e98d7199db59da97357e37e3ec9c41e9fc337fd530d1b -->

```json
{
  "consumers": [
    "a-riverhog-cli"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-cli:environment:A_RIVERHOG_CLI_LOCAL_ROOT",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_CLI_LOCAL_ROOT",
  "owner": "a-riverhog-cli"
}
```

</details>
