# A_RIVERHOG_AWS_STORE_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-config:bcbca8d7cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1c3c820c2c"></a>

| Field | Value |
|---|---|
| <a id="s-aca3251563"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-210c2b46d5"></a>`default_expressions` | `["unset"]` |
| <a id="s-0463c6ecc3"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CONFIG"` |
| <a id="s-36a54fae1a"></a>`input_shape` | `"environment-string"` |
| <a id="s-5945522bb9"></a>`name` | `"A_RIVERHOG_AWS_STORE_CONFIG"` |
| <a id="s-8beef43a66"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-849c4febaa"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_CONFIG](../../../evidence/sources/authorities.md#src-81acd40fdf) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}CONFIG')` |

### Machine authority

- `/external_contract/configuration_environment/36`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7e4813b23df0e47ae2fe2bca551b09c027b5f71cb2e71cca54037f6f46ab2f5 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CONFIG",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_CONFIG",
  "owner": "a-riverhog-aws-store"
}
```

</details>
