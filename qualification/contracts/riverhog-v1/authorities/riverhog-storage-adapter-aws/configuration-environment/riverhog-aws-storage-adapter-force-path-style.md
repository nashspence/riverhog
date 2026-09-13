# RIVERHOG_AWS_STORAGE_ADAPTER_FORCE_PATH_STYLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-force-path-style:2160e2fd07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b5b56da79c"></a>
| Field | Shape |
|---|---|
| <a id="s-c4c426b9e4"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-3371f237e2"></a>`default_expressions` | ["''"] |
| <a id="s-f366076fc7"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_FORCE_PATH_STYLE" |
| <a id="s-8ecc30e0a1"></a>`input_shape` | "environment-string" |
| <a id="s-953e2f4a91"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_FORCE_PATH_STYLE" |
| <a id="s-ba60d64f0b"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-e0ca48323b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_FORCE_PATH_STYLE](../../../evidence/sources.md#src-64b0aa7cef) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/94`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 796c139299aec9847df6c051cbb6ed73669dbbbf3255d6f892ad3fc4600575b8 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_FORCE_PATH_STYLE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_FORCE_PATH_STYLE",
  "owner": "riverhog-storage-adapter-aws"
}
```
