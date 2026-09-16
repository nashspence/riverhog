# RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-token-file:dc9bc40e56 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6d8ed99c25"></a>

| Field | Value |
|---|---|
| <a id="s-455d12eaca"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-1072ddffa8"></a>`default_expressions` | `["unset"]` |
| <a id="s-70cf382471"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN_FILE"` |
| <a id="s-1b2f5581d2"></a>`input_shape` | `"environment-string"` |
| <a id="s-1b616d0548"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN_FILE"` |
| <a id="s-8d577f9560"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-d375c8fe82"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN_FILE](../../../evidence/sources.md#src-86a691ce39) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/114`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6342e0e27926ffaf0006795c3f79d3f372a2d5f02bb43a78cb4a13571749ec3 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN_FILE",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
