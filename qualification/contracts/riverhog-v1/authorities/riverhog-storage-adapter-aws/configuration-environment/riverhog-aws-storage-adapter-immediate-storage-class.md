# RIVERHOG_AWS_STORAGE_ADAPTER_IMMEDIATE_STORAGE_CLASS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-immediate-storage-class:dccf6c8078 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6e5c2a5e00"></a>

| Field | Value |
|---|---|
| <a id="s-8adc3e5c22"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-76a1552434"></a>`default_expressions` | `["''"]` |
| <a id="s-61aa35b3fe"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_IMMEDIATE_STORAGE_CLASS"` |
| <a id="s-eb67453ab2"></a>`input_shape` | `"environment-string"` |
| <a id="s-bc8e0830d2"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_IMMEDIATE_STORAGE_CLASS"` |
| <a id="s-8e15fbe367"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-c9272554d2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_IMMEDIATE_STORAGE_CLASS](../../../evidence/sources.md#src-1f7b750083) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/96`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b7efa0a810ae62f1138bc091103819bbeb39f3f91d0e695dc84f06a226ab3e5 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_IMMEDIATE_STORAGE_CLASS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_IMMEDIATE_STORAGE_CLASS",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
