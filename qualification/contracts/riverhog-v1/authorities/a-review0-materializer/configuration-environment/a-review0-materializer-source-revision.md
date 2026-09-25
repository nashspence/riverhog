# A_REVIEW0_MATERIALIZER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-source-revision:d470af922e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e342da9aef"></a>

| Field | Value |
|---|---|
| <a id="s-1e916619c8"></a>`consumers` | `["a-review0-materializer"]` |
| <a id="s-fa5f6c460e"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-6907510a84"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_SOURCE_REVISION"` |
| <a id="s-f2a6bcf8cf"></a>`input_shape` | `"environment-string"` |
| <a id="s-89688e57e4"></a>`name` | `"A_REVIEW0_MATERIALIZER_SOURCE_REVISION"` |
| <a id="s-b6d0b64384"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-e5f9f3bbe3"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_SOURCE_REVISION](../../../evidence/sources/authorities.md#src-e2f6e628a4) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::main](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/9`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae16a04c6f63b2a141338aedc4a62eb6c58a69ea50ee4414e8172c6f7fa5c31b -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_SOURCE_REVISION",
  "owner": "a-review0-materializer"
}
```

</details>
