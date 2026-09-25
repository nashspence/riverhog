# A_REVIEW0_MATERIALIZER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-host:95d09f8a35 -->

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
| <a id="s-ba39d61b87"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-3b7a45f265"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_HOST"` |
| <a id="s-0cfd103afc"></a>`input_shape` | `"environment-string"` |
| <a id="s-e26009ba61"></a>`name` | `"A_REVIEW0_MATERIALIZER_HOST"` |
| <a id="s-ba143c7f61"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-7a684e8a7a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_HOST](../../../evidence/sources/authorities.md#src-207502149d) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::\_parser](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/6`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2e83879e40476f6f452bb28df1b8ef0fdf3f970ab7ab97966b2e2fc483ae142 -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_HOST",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_HOST",
  "owner": "a-review0-materializer"
}
```

</details>
