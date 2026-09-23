# A_RIVERHOG_AWS_STORE_READ_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-read-mode:4a78e4cca0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bff615c87e"></a>

| Field | Value |
|---|---|
| <a id="s-b6aa311db5"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-62067d9431"></a>`default_expressions` | `["''"]` |
| <a id="s-27e6fb1e12"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_READ_MODE"` |
| <a id="s-1e94bb914a"></a>`input_shape` | `"environment-string"` |
| <a id="s-6be1a5a47c"></a>`name` | `"A_RIVERHOG_AWS_STORE_READ_MODE"` |
| <a id="s-cf178005dd"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-e9a37542bb"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_READ_MODE](../../../evidence/sources/authorities.md#src-8f34648e37) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/62`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0adbdba7cf5291d86ba6abed8e57d2d99196dec24b2f6e4378cf355092d5f692 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_READ_MODE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_READ_MODE",
  "owner": "a-riverhog-aws-store"
}
```

</details>
