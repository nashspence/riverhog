# A_RIVERHOG_AWS_STORE_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-port:6b804cc0fb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c6af80cc8d"></a>

| Field | Value |
|---|---|
| <a id="s-dc771f99e6"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-119b2b8dfe"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-a725fad84c"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_PORT"` |
| <a id="s-9285c08ab7"></a>`input_shape` | `"environment-string"` |
| <a id="s-3869830d71"></a>`name` | `"A_RIVERHOG_AWS_STORE_PORT"` |
| <a id="s-ff15d2537d"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-b331e68d6a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_PORT](../../../evidence/sources/authorities.md#src-73257edd0f) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_parser](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/38`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc6a6f1ad79dd218b77d5c9ccb10b78cfecdece761d46cdbe4b5476fd92b6c39 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_PORT",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_PORT",
  "owner": "a-riverhog-aws-store"
}
```

</details>
