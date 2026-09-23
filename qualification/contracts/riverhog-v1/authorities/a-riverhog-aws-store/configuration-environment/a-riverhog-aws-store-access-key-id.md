# A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-access-key-id:4cca3fb43f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c5d59cd44b"></a>

| Field | Value |
|---|---|
| <a id="s-4676feab29"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-5123e633a7"></a>`default_expressions` | `["unset"]` |
| <a id="s-8e21124594"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID"` |
| <a id="s-22a7d99444"></a>`input_shape` | `"environment-string"` |
| <a id="s-0774de54c0"></a>`name` | `"A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID"` |
| <a id="s-020c7424d1"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-a807f64a6b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID](../../../evidence/sources/authorities.md#src-2c8960c05c) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/46`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: babcbc35907a0bbf54ed612b2e4dbf859506f79467b78cf0f9b578093ae67904 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID",
  "owner": "a-riverhog-aws-store"
}
```

</details>
