# A_RIVERHOG_AWS_STORE_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-host:ced9eb12d0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-96459587f4"></a>

| Field | Value |
|---|---|
| <a id="s-77d8284e62"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-a4ea8ca34c"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-db2f70ffc3"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_HOST"` |
| <a id="s-f6de587738"></a>`input_shape` | `"environment-string"` |
| <a id="s-a4e0febe13"></a>`name` | `"A_RIVERHOG_AWS_STORE_HOST"` |
| <a id="s-efcd10c3b8"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-c4574c4c7b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

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

- `/external_contract/configuration_environment/56`

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
