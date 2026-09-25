# TERM

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-cli:term:1e743ae3dc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6d67d56ce0"></a>

| Field | Value |
|---|---|
| <a id="s-1a3e8ba501"></a>`consumers` | `["a-riverhog-cli"]` |
| <a id="s-303afe34c5"></a>`default_expressions` | `["unset"]` |
| <a id="s-c1495d444f"></a>`id` | `"a-riverhog-cli:environment:TERM"` |
| <a id="s-a4570fd55b"></a>`input_shape` | `"environment-string"` |
| <a id="s-639a4bef7f"></a>`name` | `"TERM"` |
| <a id="s-a908c852ad"></a>`owner` | `"a-riverhog-cli"` |

## Governing policies

- <a id="pa-f3bfa901a4"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-cli:TERM](../../../evidence/sources/authorities.md#src-b2d6ead1ab) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/cli\_support.py::plain\_output\_requested](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/cli_support.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-cli` | [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/cli\_support.py](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/cli_support.py) | `os.getenv('TERM')` |

### Machine authority

- `/external_contract/configuration_environment/48`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9661eb8fd04983c87f3e23c46dfec97bf6a2b964881eb210bd646bb0d5912b40 -->

```json
{
  "consumers": [
    "a-riverhog-cli"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-cli:environment:TERM",
  "input_shape": "environment-string",
  "name": "TERM",
  "owner": "a-riverhog-cli"
}
```

</details>
