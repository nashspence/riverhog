# A_REVIEW0_MATERIALIZER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-token:ee0635cc71 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b6148dfd81"></a>

| Field | Value |
|---|---|
| <a id="s-b49faf4a7f"></a>`consumers` | `["a-review0-materializer"]` |
| <a id="s-e4135fc5a3"></a>`default_expressions` | `["unset"]` |
| <a id="s-23f3a2f19a"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_TOKEN"` |
| <a id="s-41c7764688"></a>`input_shape` | `"environment-string"` |
| <a id="s-a2dcbb91e6"></a>`name` | `"A_REVIEW0_MATERIALIZER_TOKEN"` |
| <a id="s-392cf8391f"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-a066e3f696"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_TOKEN](../../../evidence/sources/authorities.md#src-84c5b4c59e) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::\_secret](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py); [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::main](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_TOKEN')` |
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.environ.pop(f'{PREFIX}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/12`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 204487c45e0c410bf1856a251f6757bdacb8e42c672153e417409777056e2adf -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_TOKEN",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_TOKEN",
  "owner": "a-review0-materializer"
}
```

</details>
