# A_RIVERHOG_AWS_STORE_SECRET_ACCESS_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-secret-access-key:42789ec738 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-63134bf139"></a>

| Field | Value |
|---|---|
| <a id="s-7a2481d8f8"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-47a10e73f7"></a>`default_expressions` | `["unset"]` |
| <a id="s-d1248cce4c"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_SECRET_ACCESS_KEY"` |
| <a id="s-970820b4ef"></a>`input_shape` | `"environment-string"` |
| <a id="s-822be7b829"></a>`name` | `"A_RIVERHOG_AWS_STORE_SECRET_ACCESS_KEY"` |
| <a id="s-3c8df38f70"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-811e24c06e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_SECRET_ACCESS_KEY](../../../evidence/sources/authorities.md#src-7158ed4102) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/69`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b48f0e1c6f73e76c7a0a32acd2d78537c9fa6da431d1311eefc7c43f8a43921e -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_SECRET_ACCESS_KEY",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_SECRET_ACCESS_KEY",
  "owner": "a-riverhog-aws-store"
}
```

</details>
