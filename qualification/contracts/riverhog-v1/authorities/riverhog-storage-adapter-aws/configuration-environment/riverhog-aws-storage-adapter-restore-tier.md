# RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_TIER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-restore-tier:e1eb601b67 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-2034712ffe) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5f56b9ebf5"></a>
| Field | Shape |
|---|---|
| <a id="s-451b5d705f"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-f6a8f3b6c7"></a>`default_expressions` | ["''"] |
| <a id="s-1b1b7d2c5f"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_TIER" |
| <a id="s-43982f1d97"></a>`input_shape` | "environment-string" |
| <a id="s-7dbd023a7b"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_TIER" |
| <a id="s-d0a5790f47"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-06e9c42bd9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_TIER](../../../evidence/sources.md#src-15442d9633) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/105`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c89143ced9450f122c795fec24d1b49290847af527c4b7f23fdd570088125e11 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_TIER",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_TIER",
  "owner": "riverhog-storage-adapter-aws"
}
```
