# RIVERHOG_AWS_STORAGE_ADAPTER_ROOT_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-root-prefix:9e2f479349 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b002419278"></a>
| Field | Shape |
|---|---|
| <a id="s-e3ac27b7d7"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-a3b4239e3f"></a>`default_expressions` | ["''"] |
| <a id="s-42e9fbb861"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ROOT_PREFIX" |
| <a id="s-dc370af86b"></a>`input_shape` | "environment-string" |
| <a id="s-058d8b8874"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_ROOT_PREFIX" |
| <a id="s-46978b3ca6"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-6fc001f11b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ROOT_PREFIX](../../../evidence/sources.md#src-4960800462) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/107`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b865a3ff68563983e1f0126acb1639a173b4c3f2a6b82550eea1c6e3084b8f0 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ROOT_PREFIX",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_ROOT_PREFIX",
  "owner": "riverhog-storage-adapter-aws"
}
```
