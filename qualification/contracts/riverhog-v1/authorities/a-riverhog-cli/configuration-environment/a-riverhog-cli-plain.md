# A_RIVERHOG_CLI_PLAIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-cli:a-riverhog-cli-plain:2041e6e768 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ee975e99ad"></a>

| Field | Value |
|---|---|
| <a id="s-ca55d6dc58"></a>`consumers` | `["a-riverhog-cli"]` |
| <a id="s-306a5ba67b"></a>`default_expressions` | `["''"]` |
| <a id="s-777167e457"></a>`id` | `"a-riverhog-cli:environment:A_RIVERHOG_CLI_PLAIN"` |
| <a id="s-faf3e3ddf2"></a>`input_shape` | `"environment-string"` |
| <a id="s-1a8ef76883"></a>`name` | `"A_RIVERHOG_CLI_PLAIN"` |
| <a id="s-1095e66092"></a>`owner` | `"a-riverhog-cli"` |

## Governing policies

- <a id="pa-94c1a80e4f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-cli:A_RIVERHOG_CLI_PLAIN](../../../evidence/sources/authorities.md#src-5c9f619165) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/cli\_support.py::plain\_output\_requested](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/cli_support.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-cli` | [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/cli\_support.py](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/cli_support.py) | `os.getenv(setting, '')` |

### Machine authority

- `/external_contract/configuration_environment/44`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fe657a9c17081e1eb9cfc6ff75ec35adbd11a92f13b5998e673b929b51434f2 -->

```json
{
  "consumers": [
    "a-riverhog-cli"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-cli:environment:A_RIVERHOG_CLI_PLAIN",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_CLI_PLAIN",
  "owner": "a-riverhog-cli"
}
```

</details>
