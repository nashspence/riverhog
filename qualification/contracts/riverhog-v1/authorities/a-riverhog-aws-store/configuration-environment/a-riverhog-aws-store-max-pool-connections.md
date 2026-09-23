# A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-max-pool-connections:29da69a44c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-749bd6e141"></a>

| Field | Value |
|---|---|
| <a id="s-032458916b"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-595d4c8a28"></a>`default_expressions` | `["''"]` |
| <a id="s-6b131cbb88"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS"` |
| <a id="s-deda87c07c"></a>`input_shape` | `"environment-string"` |
| <a id="s-94ad8621b7"></a>`name` | `"A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS"` |
| <a id="s-c4e479ca84"></a>`owner` | `"a-riverhog-aws-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS"; consumers=["a-riverhog-aws-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS](#s-749bd6e141) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-39f46cda7b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-33fb940e34"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS](../../../evidence/sources/authorities.md#src-64871b8aa9) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/59`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca4179cc5ea04e23f9a29fd43e8e5ad8bbbe6b7384a560813a5851208b22a201 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_MAX_POOL_CONNECTIONS",
  "owner": "a-riverhog-aws-store"
}
```

</details>
