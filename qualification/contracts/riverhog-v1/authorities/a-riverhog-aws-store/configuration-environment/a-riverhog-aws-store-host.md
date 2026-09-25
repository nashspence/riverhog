# A_RIVERHOG_AWS_STORE_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-host:693dc97eb6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ee8d4c5bb2"></a>

| Field | Value |
|---|---|
| <a id="s-9cef198e9c"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-4e914a87ae"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-c87995acf3"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_HOST"` |
| <a id="s-1c5840c446"></a>`input_shape` | `"environment-string"` |
| <a id="s-199cea8e22"></a>`name` | `"A_RIVERHOG_AWS_STORE_HOST"` |
| <a id="s-a35e48e93a"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-3205654851"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_HOST](../../../evidence/sources/authorities.md#src-e80b49116c) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/37`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a5c8f90c223fc122743af40865f5b0afe77fb69f5a07d1e02d06ec16321b509 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_HOST",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_HOST",
  "owner": "a-riverhog-aws-store"
}
```

</details>
