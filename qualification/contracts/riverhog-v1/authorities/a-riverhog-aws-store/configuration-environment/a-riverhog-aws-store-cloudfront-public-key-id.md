# A_RIVERHOG_AWS_STORE_CLOUDFRONT_PUBLIC_KEY_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-cloudfront-public-key-id:189745a4aa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ef146f919d"></a>

| Field | Value |
|---|---|
| <a id="s-93a23b729e"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-4a3b51c44c"></a>`default_expressions` | `["''"]` |
| <a id="s-7df70bf7e9"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CLOUDFRONT_PUBLIC_KEY_ID"` |
| <a id="s-202888fb55"></a>`input_shape` | `"environment-string"` |
| <a id="s-4e7ddbc9fe"></a>`name` | `"A_RIVERHOG_AWS_STORE_CLOUDFRONT_PUBLIC_KEY_ID"` |
| <a id="s-3e6453e97a"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-26721459b4"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_CLOUDFRONT_PUBLIC_KEY_ID](../../../evidence/sources/authorities.md#src-bf79131754) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/52`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: daffd178e1a036ed407e12e4a8ea21831cf483aa6ef045f9ab2190d800016c3f -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CLOUDFRONT_PUBLIC_KEY_ID",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_CLOUDFRONT_PUBLIC_KEY_ID",
  "owner": "a-riverhog-aws-store"
}
```

</details>
