# A_RIVERHOG_AWS_STORE_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-token-file:cfe0208215 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-80acbe8bb3"></a>

| Field | Value |
|---|---|
| <a id="s-63d41d35b6"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-698b9afc79"></a>`default_expressions` | `["unset"]` |
| <a id="s-6991f7b9b9"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_TOKEN_FILE"` |
| <a id="s-0783a8b801"></a>`input_shape` | `"environment-string"` |
| <a id="s-8091a081fc"></a>`name` | `"A_RIVERHOG_AWS_STORE_TOKEN_FILE"` |
| <a id="s-cc5e961af0"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-41cd584ff2"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_TOKEN_FILE](../../../evidence/sources/authorities.md#src-e9cf005c33) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/75`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c313626298000dd1c85e1a988baee31c8bbc42598165ae55542d0967f29e5ebc -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_TOKEN_FILE",
  "owner": "a-riverhog-aws-store"
}
```

</details>
