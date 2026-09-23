# A_RIVERHOG_AWS_STORE_CLOUDFRONT_PRIVATE_KEY_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-cloudfront-private-key-path:5873a08fbf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9857458ffa"></a>

| Field | Value |
|---|---|
| <a id="s-fe693377ae"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-0473aebcab"></a>`default_expressions` | `["''"]` |
| <a id="s-8cbdf403cb"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CLOUDFRONT_PRIVATE_KEY_PATH"` |
| <a id="s-7b2e5c71b6"></a>`input_shape` | `"environment-string"` |
| <a id="s-03c589148c"></a>`name` | `"A_RIVERHOG_AWS_STORE_CLOUDFRONT_PRIVATE_KEY_PATH"` |
| <a id="s-0666993cd6"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-1d94c2a496"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_CLOUDFRONT_PRIVATE_KEY_PATH](../../../evidence/sources/authorities.md#src-4bcc4fb100) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/51`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2abfcea4466cde31f8f00d9dd6eb38e53b7a9b53e91cdc59b3183306b2bf7a86 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CLOUDFRONT_PRIVATE_KEY_PATH",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_CLOUDFRONT_PRIVATE_KEY_PATH",
  "owner": "a-riverhog-aws-store"
}
```

</details>
