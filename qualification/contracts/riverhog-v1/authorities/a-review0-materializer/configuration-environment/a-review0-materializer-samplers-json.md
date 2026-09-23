# A_REVIEW0_MATERIALIZER_SAMPLERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-samplers-json:ab06210c4e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1966a8d0ec"></a>

| Field | Value |
|---|---|
| <a id="s-9a2311e6cd"></a>`consumers` | `["a-review0-materializer"]` |
| <a id="s-6b12b9694a"></a>`default_expressions` | `["unset"]` |
| <a id="s-cf3af0a6fd"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_SAMPLERS_JSON"` |
| <a id="s-0f9cc1851c"></a>`input_shape` | `"environment-string"` |
| <a id="s-4341315209"></a>`name` | `"A_REVIEW0_MATERIALIZER_SAMPLERS_JSON"` |
| <a id="s-4ea39658ad"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-f23a8c9a19"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_SAMPLERS_JSON](../../../evidence/sources/authorities.md#src-8d80b0baad) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::\_sampler\_registrations](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_SAMPLERS_JSON')` |

### Machine authority

- `/external_contract/configuration_environment/8`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6da70c478bae60d3bcac46be3ba432cc8442eab03dd55d51b54f918fa9b6704 -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_SAMPLERS_JSON",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_SAMPLERS_JSON",
  "owner": "a-review0-materializer"
}
```

</details>
