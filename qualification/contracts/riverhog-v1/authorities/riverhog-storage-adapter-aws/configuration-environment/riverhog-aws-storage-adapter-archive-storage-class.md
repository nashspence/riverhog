# RIVERHOG_AWS_STORAGE_ADAPTER_ARCHIVE_STORAGE_CLASS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-archive-storage-class:c1e7ccbd45 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-41e02220a1"></a>
| Field | Shape |
|---|---|
| <a id="s-84903db1b5"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-305c1a2132"></a>`default_expressions` | ["''"] |
| <a id="s-db7c90b69a"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ARCHIVE_STORAGE_CLASS" |
| <a id="s-4c733e3e28"></a>`input_shape` | "environment-string" |
| <a id="s-f3010b390a"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_ARCHIVE_STORAGE_CLASS" |
| <a id="s-8d1cbb3d58"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-09663c3bc5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ARCHIVE_STORAGE_CLASS](../../../evidence/sources.md#src-c7af32e338) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/87`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b62f4c0ba9185149f30575de668b1f5fed2a1d1c96742e8936c6c1d4d87c699c -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ARCHIVE_STORAGE_CLASS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_ARCHIVE_STORAGE_CLASS",
  "owner": "riverhog-storage-adapter-aws"
}
```
