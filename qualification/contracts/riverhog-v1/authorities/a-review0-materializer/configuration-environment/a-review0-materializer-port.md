# A_REVIEW0_MATERIALIZER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-materializer:a-review0-materializer-port:b732b4accb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2436bad7d4"></a>

| Field | Value |
|---|---|
| <a id="s-478f37a840"></a>`consumers` | `["a-review0-materializer"]` |
| <a id="s-a9c79e933b"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-7ed31707ad"></a>`id` | `"a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_PORT"` |
| <a id="s-d5950a2bc2"></a>`input_shape` | `"environment-string"` |
| <a id="s-7d2e712aed"></a>`name` | `"A_REVIEW0_MATERIALIZER_PORT"` |
| <a id="s-cfbebc15f6"></a>`owner` | `"a-review0-materializer"` |

## Governing policies

- <a id="pa-0f694302d9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-materializer:A_REVIEW0_MATERIALIZER_PORT](../../../evidence/sources/authorities.md#src-7df683d643) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::\_parser](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-materializer` | [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py) | `os.getenv(f'{PREFIX}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/7`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f826f826ae76e0d42b9e141d0b2d175f03cefcb75b53d02457b785e27cfb26ee -->

```json
{
  "consumers": [
    "a-review0-materializer"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-review0-materializer:environment:A_REVIEW0_MATERIALIZER_PORT",
  "input_shape": "environment-string",
  "name": "A_REVIEW0_MATERIALIZER_PORT",
  "owner": "a-review0-materializer"
}
```

</details>
