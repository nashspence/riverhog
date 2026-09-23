# A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-connect-timeout-seconds:3e50e581ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1cd6fe0a97"></a>

| Field | Value |
|---|---|
| <a id="s-923821698e"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-31873541f2"></a>`default_expressions` | `["''"]` |
| <a id="s-a7a2756f2e"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS"` |
| <a id="s-f7fb27f6db"></a>`input_shape` | `"environment-string"` |
| <a id="s-8f98162f43"></a>`name` | `"A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS"` |
| <a id="s-5f637bee10"></a>`owner` | `"a-riverhog-aws-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS"; consumers=["a-riverhog-aws-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS](#s-1cd6fe0a97) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-296cbb8840"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-a201da5992"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-046388cbe5) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/53`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16fe0050f20968a74e0eb207a7fa3e98dc8ae05c876ebdbc3b537440e4909b5a -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_CONNECT_TIMEOUT_SECONDS",
  "owner": "a-riverhog-aws-store"
}
```

</details>
