# RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-access-key-id:8f149e3884 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8a57472b3d"></a>
| Field | Shape |
|---|---|
| <a id="s-8b87502075"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-9e470cf17e"></a>`default_expressions` | ["unset"] |
| <a id="s-0d275a9d51"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID" |
| <a id="s-f9b9e2d406"></a>`input_shape` | "environment-string" |
| <a id="s-ebf779a82f"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID" |
| <a id="s-0db1e92c3c"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-9e47a2ba1f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID](../../../evidence/sources.md#src-79fd55b300) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.environ.pop(direct_name)` |
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/85`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ba678d579e2e1f9225e05b5debbd069dad0bf78d64bb416051ec0a7efcd5532 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID",
  "owner": "riverhog-storage-adapter-aws"
}
```
