# A_RIVERHOG_AWS_STORE_FORCE_PATH_STYLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-force-path-style:e31f1b30f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-021597da78"></a>

| Field | Value |
|---|---|
| <a id="s-de450a9408"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-1d23c0d90d"></a>`default_expressions` | `["''"]` |
| <a id="s-7b17b012fe"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_FORCE_PATH_STYLE"` |
| <a id="s-487e8c583c"></a>`input_shape` | `"environment-string"` |
| <a id="s-9d016ce2fa"></a>`name` | `"A_RIVERHOG_AWS_STORE_FORCE_PATH_STYLE"` |
| <a id="s-1cfe26331e"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-b060015867"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_FORCE_PATH_STYLE](../../../evidence/sources/authorities.md#src-1d07ef1da0) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/55`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c95e699c84332f6f207fd04f3dde04410b88108caa46dddb80c7aeb26c342ef -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_FORCE_PATH_STYLE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_FORCE_PATH_STYLE",
  "owner": "a-riverhog-aws-store"
}
```

</details>
