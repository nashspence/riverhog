# A_RIVERHOG_AWS_STORE_SESSION_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-session-token:33fee8a9bd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c992f01b6c"></a>

| Field | Value |
|---|---|
| <a id="s-ad7e41feee"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-3b77a1770c"></a>`default_expressions` | `["unset"]` |
| <a id="s-82330ee13f"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_SESSION_TOKEN"` |
| <a id="s-15b98567cc"></a>`input_shape` | `"environment-string"` |
| <a id="s-4a3181eea5"></a>`name` | `"A_RIVERHOG_AWS_STORE_SESSION_TOKEN"` |
| <a id="s-e9681c4a64"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-dbc642c131"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_SESSION_TOKEN](../../../evidence/sources/authorities.md#src-6308b13796) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional\_secret](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.environ.pop(direct_name)` |
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/71`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0afeef679ed68808a92c4606456eecad75649e0f304a9e6fe634c2c3d94036da -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_SESSION_TOKEN",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_SESSION_TOKEN",
  "owner": "a-riverhog-aws-store"
}
```

</details>
