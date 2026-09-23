# A_RIVERHOG_AWS_STORE_IMMEDIATE_STORAGE_CLASS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-immediate-storage-class:7eb8fa4fc3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-79f8ec8b66"></a>

| Field | Value |
|---|---|
| <a id="s-3b66af5af9"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-f033f04804"></a>`default_expressions` | `["''"]` |
| <a id="s-523002be62"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_IMMEDIATE_STORAGE_CLASS"` |
| <a id="s-08d11a5f6d"></a>`input_shape` | `"environment-string"` |
| <a id="s-af9e9db7e5"></a>`name` | `"A_RIVERHOG_AWS_STORE_IMMEDIATE_STORAGE_CLASS"` |
| <a id="s-228a0a0fe8"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-a991063d1d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_IMMEDIATE_STORAGE_CLASS](../../../evidence/sources/authorities.md#src-9380043eaf) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/57`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a106835c471cfc2fee367615c60f58577394524a573aa45a5c2a8dd8fbe7a5e8 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_IMMEDIATE_STORAGE_CLASS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_IMMEDIATE_STORAGE_CLASS",
  "owner": "a-riverhog-aws-store"
}
```

</details>
