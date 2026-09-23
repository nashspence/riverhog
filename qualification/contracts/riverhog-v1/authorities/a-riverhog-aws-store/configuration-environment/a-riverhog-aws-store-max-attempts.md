# A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-max-attempts:c6530d531a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-53a88dfb96"></a>

| Field | Value |
|---|---|
| <a id="s-cad8bbbd17"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-dadceebea3"></a>`default_expressions` | `["''"]` |
| <a id="s-dbe47075d5"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS"` |
| <a id="s-f6d00c3d7b"></a>`input_shape` | `"environment-string"` |
| <a id="s-c305899a87"></a>`name` | `"A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS"` |
| <a id="s-02d8846ea8"></a>`owner` | `"a-riverhog-aws-store"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS"; consumers=["a-riverhog-aws-store"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS](#s-53a88dfb96) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8d72a0c71c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-35414ea411"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS](../../../evidence/sources/authorities.md#src-73c06134f9) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/58`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f4b571c3f810a717eda94a10ba263266ef59be3719d28d70b6c31f405f5925fb -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_MAX_ATTEMPTS",
  "owner": "a-riverhog-aws-store"
}
```

</details>
