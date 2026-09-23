# A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-access-key-id-file:b2c4842703 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5581cd63e1"></a>

| Field | Value |
|---|---|
| <a id="s-77d05a28fd"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-64e0770d83"></a>`default_expressions` | `["unset"]` |
| <a id="s-f39456d089"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID_FILE"` |
| <a id="s-d4011511cf"></a>`input_shape` | `"environment-string"` |
| <a id="s-f0549843d7"></a>`name` | `"A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID_FILE"` |
| <a id="s-4055a76d48"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-fce335944e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID_FILE](../../../evidence/sources/authorities.md#src-3107f2bb23) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_secret](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/47`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0b6b36a2f1f0d97222392a16f856816638710e110eb9e56d915347774d358e3 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID_FILE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID_FILE",
  "owner": "a-riverhog-aws-store"
}
```

</details>
