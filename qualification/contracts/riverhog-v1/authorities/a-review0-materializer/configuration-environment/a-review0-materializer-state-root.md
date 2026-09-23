# A_REVIEW0_MATERIALIZER_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-state-root:6e1ba0d6a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8f9926b885"></a>

| Field | Value |
|---|---|
| <a id="s-9172f4f7f0"></a>`consumers` | `["a-review0-materializer"]` |
| <a id="s-0f0e40311d"></a>`default_expressions` | `["'/var/lib/a-review0-materializer'"]` |
| <a id="s-4e688baa7f"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_STATE_ROOT"` |
| <a id="s-25ca4126b5"></a>`input_shape` | `"environment-string"` |
| <a id="s-1fd2f143d5"></a>`name` | `"A_REVIEW0_MATERIALIZER_STATE_ROOT"` |
| <a id="s-42b278dcfa"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-969c9f4ca8"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_STATE_ROOT](../../../evidence/sources/authorities.md#src-50b55c386d) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::main](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_STATE_ROOT', '/var/lib/a-review0-materializer')` |

### Machine authority

- `/external_contract/configuration_environment/11`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 441f3ab79f6a3a99188fc730022b0ac85bc54113e375efb974fc05c90ae9836b -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "'/var/lib/a-review0-materializer'"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_STATE_ROOT",
  "owner": "a-review0-materializer"
}
```

</details>
