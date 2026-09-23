# A_RIVERHOG_AWS_STORE_BUCKET

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-bucket:7cfdbe20e7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-be3c1ab328"></a>

| Field | Value |
|---|---|
| <a id="s-53a8b95257"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-92b359eb99"></a>`default_expressions` | `["''"]` |
| <a id="s-0695702476"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_BUCKET"` |
| <a id="s-204d487369"></a>`input_shape` | `"environment-string"` |
| <a id="s-e79d9952b9"></a>`name` | `"A_RIVERHOG_AWS_STORE_BUCKET"` |
| <a id="s-5894963f53"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-f97a052fbb"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_BUCKET](../../../evidence/sources/authorities.md#src-e3ebe113b4) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_required](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/49`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ddaef5192c497581d9fd8cf443d4b1b7d6dcf603f31854851bf51f1ee2cb1fe9 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_BUCKET",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_BUCKET",
  "owner": "a-riverhog-aws-store"
}
```

</details>
