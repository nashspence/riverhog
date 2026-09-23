# A_RIVERHOG_AWS_STORE_REGION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-region:4e51f34807 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1c368e3bbb"></a>

| Field | Value |
|---|---|
| <a id="s-0e717d7f5c"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-ed83d1071c"></a>`default_expressions` | `["''"]` |
| <a id="s-8acc408344"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_REGION"` |
| <a id="s-18b068f2a0"></a>`input_shape` | `"environment-string"` |
| <a id="s-561f90128c"></a>`name` | `"A_RIVERHOG_AWS_STORE_REGION"` |
| <a id="s-2750bfabee"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-131e175eaa"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_REGION](../../../evidence/sources/authorities.md#src-455030544a) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_required](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/64`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 694ce4d3f5ffc78370302364be697a0e3707ac1e6f8f17e2b1c9234dd0b1e9ca -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_REGION",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_REGION",
  "owner": "a-riverhog-aws-store"
}
```

</details>
