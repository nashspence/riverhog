# A_REVIEW0_MATERIALIZER_IMAGE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-image-id:c59c5d8338 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-70ab20d602"></a>

| Field | Value |
|---|---|
| <a id="s-cf5ca173ed"></a>`consumers` | `["a-review0-materializer"]` |
| <a id="s-ba39d61b87"></a>`default_expressions` | `["''"]` |
| <a id="s-3b7a45f265"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_IMAGE_ID"` |
| <a id="s-0cfd103afc"></a>`input_shape` | `"environment-string"` |
| <a id="s-e26009ba61"></a>`name` | `"A_REVIEW0_MATERIALIZER_IMAGE_ID"` |
| <a id="s-ba143c7f61"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-760ff53b64"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_IMAGE_ID](../../../evidence/sources/authorities.md#src-f946471744) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::\_image\_id](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_IMAGE_ID', '')` |

### Machine authority

- `/external_contract/configuration_environment/6`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfcd39019031c1125161560844fc6778d5ff1c6850101e5422d72a126a76aff7 -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_IMAGE_ID",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_IMAGE_ID",
  "owner": "a-review0-materializer"
}
```

</details>
