# A_RIVERHOG_AWS_STORE_RETRY_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-retry-mode:abb333aec2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-a44ff15aa7"></a>

| Field | Value |
|---|---|
| <a id="s-cbfaedd8f7"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-2b9491bcda"></a>`default_expressions` | `["''"]` |
| <a id="s-2908730b97"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_RETRY_MODE"` |
| <a id="s-96390f5c80"></a>`input_shape` | `"environment-string"` |
| <a id="s-394d41cd98"></a>`name` | `"A_RIVERHOG_AWS_STORE_RETRY_MODE"` |
| <a id="s-436907c2b0"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-b7f1493607"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_RETRY_MODE](../../../evidence/sources/authorities.md#src-6e989e73ba) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/67`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f916e9281964b616da3f4653e3b7f153b85dc72f75770baa91a9bba6a782ffc5 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_RETRY_MODE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_RETRY_MODE",
  "owner": "a-riverhog-aws-store"
}
```

</details>
