# RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-read-timeout-seconds:2497dfb9e7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-663ea2a8c9"></a>

| Field | Value |
|---|---|
| <a id="s-e37a73aa7a"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-867ee31ced"></a>`default_expressions` | `["''"]` |
| <a id="s-9b8c32cdd8"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS"` |
| <a id="s-bc80da0d55"></a>`input_shape` | `"environment-string"` |
| <a id="s-f850425dcc"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS"` |
| <a id="s-79f4d1452a"></a>`owner` | `"riverhog-storage-adapter-aws"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS"; consumers=["riverhog-storage-adapter-aws"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](#s-663ea2a8c9) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-499a4e97c4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-8acc1b5543"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](../../../evidence/sources.md#src-47cfeed976) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/102`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6643e9699ff7a9d17491caafd1eeb99c29e779cf8b35fc1b5b24056f6ec1ea59 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
