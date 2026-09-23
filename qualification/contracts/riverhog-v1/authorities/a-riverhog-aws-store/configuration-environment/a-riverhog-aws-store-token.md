# A_RIVERHOG_AWS_STORE_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-token:14bdfc23d8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e17f28d8d8"></a>

| Field | Value |
|---|---|
| <a id="s-a8f98984dc"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-1a8627f417"></a>`default_expressions` | `["unset"]` |
| <a id="s-e1a39d2288"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_TOKEN"` |
| <a id="s-49efae42e7"></a>`input_shape` | `"environment-string"` |
| <a id="s-72292b5725"></a>`name` | `"A_RIVERHOG_AWS_STORE_TOKEN"` |
| <a id="s-5a79a41389"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-e6465187af"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_TOKEN](../../../evidence/sources/authorities.md#src-1adfcc6598) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/74`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9329a9f468902542aaddc8256f1e14585fafcbb9da5984a9c318e888c24da13e -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_TOKEN",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_TOKEN",
  "owner": "a-riverhog-aws-store"
}
```

</details>
