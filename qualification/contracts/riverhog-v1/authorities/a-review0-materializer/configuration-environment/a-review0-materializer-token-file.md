# A_REVIEW0_MATERIALIZER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-token-file:56dd276c98 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-11a73a67cc"></a>

| Field | Value |
|---|---|
| <a id="s-56b56d915a"></a>`consumers` | `["a-review0-materializer"]` |
| <a id="s-2837f1949b"></a>`default_expressions` | `["unset"]` |
| <a id="s-05a1ce1ce4"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_TOKEN_FILE"` |
| <a id="s-a39bc19e88"></a>`input_shape` | `"environment-string"` |
| <a id="s-514e7dfc94"></a>`name` | `"A_REVIEW0_MATERIALIZER_TOKEN_FILE"` |
| <a id="s-bba7a7ca74"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-7a99f31ff1"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_TOKEN_FILE](../../../evidence/sources/authorities.md#src-3bf7c4a868) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::\_secret](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/13`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8066ea9cf316cb8be8104641fea80301bec96a289076ff40bd8426cf677933ff -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_TOKEN_FILE",
  "owner": "a-review0-materializer"
}
```

</details>
