# A_RIVERHOG_AWS_STORE_CLOUDFRONT_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-cloudfront-base-url:03ba61a203 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-71aef6f25d"></a>

| Field | Value |
|---|---|
| <a id="s-a3df3866fb"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-3fe8602d6c"></a>`default_expressions` | `["''"]` |
| <a id="s-ffb1f55176"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CLOUDFRONT_BASE_URL"` |
| <a id="s-cf559f5a8f"></a>`input_shape` | `"environment-string"` |
| <a id="s-8019296a33"></a>`name` | `"A_RIVERHOG_AWS_STORE_CLOUDFRONT_BASE_URL"` |
| <a id="s-796b4f0f4b"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-fb691227e1"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_CLOUDFRONT_BASE_URL](../../../evidence/sources/authorities.md#src-49283fc230) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/50`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af315336f0df19a2b031cfdafd0fb8150b3217cc0ec2a9baaa42f2aad64e54e3 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CLOUDFRONT_BASE_URL",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_CLOUDFRONT_BASE_URL",
  "owner": "a-riverhog-aws-store"
}
```

</details>
