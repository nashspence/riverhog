# RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PRIVATE_KEY_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-cloudfront-p-45c5882e58:73e518e1cd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-2034712ffe) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f34a6099fd"></a>
| Field | Shape |
|---|---|
| <a id="s-333cb6b464"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-855c76d1b6"></a>`default_expressions` | ["''"] |
| <a id="s-e45db274e8"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PRIVATE_KEY_PATH" |
| <a id="s-6d41d32f3a"></a>`input_shape` | "environment-string" |
| <a id="s-e8b92a27c1"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PRIVATE_KEY_PATH" |
| <a id="s-37c4cf2e50"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-d985fc639c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PRIVATE_KEY_PATH](../../../evidence/sources.md#src-4c2532ee7e) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/90`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85b5b4f141f7a274035d8674bc756f4838fd74376d37fe01dd03a458d9cecdfb -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PRIVATE_KEY_PATH",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PRIVATE_KEY_PATH",
  "owner": "riverhog-storage-adapter-aws"
}
```
