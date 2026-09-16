# RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-secret-access-key-file:ea0201ba58 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-63f2a8f8ec"></a>

| Field | Value |
|---|---|
| <a id="s-7635dc7bce"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-9adaac50e0"></a>`default_expressions` | `["unset"]` |
| <a id="s-5ba55aed0e"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE"` |
| <a id="s-74c255c80c"></a>`input_shape` | `"environment-string"` |
| <a id="s-0b8c0a3aae"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE"` |
| <a id="s-26fd49b762"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-8abec8fb3c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE](../../../evidence/sources.md#src-9390bb5416) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/109`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de3083430eac9e7c5cc569fd327a9f0a0020faa9b5202dc097d428d4d282b98d -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
