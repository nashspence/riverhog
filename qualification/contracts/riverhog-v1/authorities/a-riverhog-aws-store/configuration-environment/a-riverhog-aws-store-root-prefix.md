# A_RIVERHOG_AWS_STORE_ROOT_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-root-prefix:d3de5deb10 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3bf78de3fa"></a>

| Field | Value |
|---|---|
| <a id="s-9364d1a9f1"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-92b6a91362"></a>`default_expressions` | `["''"]` |
| <a id="s-6a8524b81a"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ROOT_PREFIX"` |
| <a id="s-594729b07d"></a>`input_shape` | `"environment-string"` |
| <a id="s-990d2b0cff"></a>`name` | `"A_RIVERHOG_AWS_STORE_ROOT_PREFIX"` |
| <a id="s-5b3245d215"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-246e2ad4d1"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_ROOT_PREFIX](../../../evidence/sources/authorities.md#src-4e874a0036) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/68`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b405a276ceee66036f39bcdf694d7b71ed9e21ccafb03b7d961333cb0551ebd1 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ROOT_PREFIX",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_ROOT_PREFIX",
  "owner": "a-riverhog-aws-store"
}
```

</details>
