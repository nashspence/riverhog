# RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-max-pool-connections:082fcf7407 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b3dd205ff4"></a>

| Field | Value |
|---|---|
| <a id="s-fdfa36834a"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-e26c4f1935"></a>`default_expressions` | `["''"]` |
| <a id="s-b929a15be2"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS"` |
| <a id="s-6d470cdf3d"></a>`input_shape` | `"environment-string"` |
| <a id="s-7a20528c0b"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS"` |
| <a id="s-211ee68554"></a>`owner` | `"riverhog-storage-adapter-aws"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS"; consumers=["riverhog-storage-adapter-aws"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](#s-b3dd205ff4) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-66dae0dcb2"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-95cb12aaa6"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](../../../evidence/sources/authorities.md#src-0419b135c2) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py::\_optional](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/98`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 406884827f7026a7be7e0e46b77be21a55f976840b981aa7f194dcd5a1516fe5 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
