# TERM

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-cli:term:578ce844a6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-663ea2a8c9"></a>

| Field | Value |
|---|---|
| <a id="s-e37a73aa7a"></a>`consumers` | `["a-riverhog-cli"]` |
| <a id="s-867ee31ced"></a>`default_expressions` | `["unset"]` |
| <a id="s-9b8c32cdd8"></a>`id` | `"a-riverhog-cli:environment:TERM"` |
| <a id="s-bc80da0d55"></a>`input_shape` | `"environment-string"` |
| <a id="s-f850425dcc"></a>`name` | `"TERM"` |
| <a id="s-79f4d1452a"></a>`owner` | `"a-riverhog-cli"` |

## Governing policies

- <a id="pa-9b31615791"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/102`

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
