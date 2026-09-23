# A_RIVERHOG_AWS_STORE_SESSION_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-session-token-file:f1c6b8fd2b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-abc6c0ac51"></a>

| Field | Value |
|---|---|
| <a id="s-1ed351fe80"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-942ac43e20"></a>`default_expressions` | `["unset"]` |
| <a id="s-d14f4cb1fe"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_SESSION_TOKEN_FILE"` |
| <a id="s-23ed4889af"></a>`input_shape` | `"environment-string"` |
| <a id="s-b3f8b0388d"></a>`name` | `"A_RIVERHOG_AWS_STORE_SESSION_TOKEN_FILE"` |
| <a id="s-92764138c3"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-d1bc1d982f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_SESSION_TOKEN_FILE](../../../evidence/sources/authorities.md#src-69369e8f96) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional\_secret](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/72`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9df8358537e6fe622c99047acd01ae801389ec3711f74a2da1c797110b3c4cba -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_SESSION_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_SESSION_TOKEN_FILE",
  "owner": "a-riverhog-aws-store"
}
```

</details>
