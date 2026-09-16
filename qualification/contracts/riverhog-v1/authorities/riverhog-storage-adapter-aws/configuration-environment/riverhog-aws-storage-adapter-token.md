# RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-token:65b462416c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bd9bd4c428"></a>

| Field | Value |
|---|---|
| <a id="s-46dd00a5c8"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-a39501b2d0"></a>`default_expressions` | `["unset"]` |
| <a id="s-d7df8d8378"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN"` |
| <a id="s-409705dbe5"></a>`input_shape` | `"environment-string"` |
| <a id="s-2f10d77123"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN"` |
| <a id="s-1bda924b26"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-ed73203eba"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN](../../../evidence/sources.md#src-4a894c7712) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.environ.pop(direct_name)` |
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/113`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be06c4b9b3eb02691167c1483df43a276674f71b708d974d6eb89497bfcae6f6 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
