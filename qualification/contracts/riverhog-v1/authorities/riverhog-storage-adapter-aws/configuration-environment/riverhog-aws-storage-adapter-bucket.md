# RIVERHOG_AWS_STORAGE_ADAPTER_BUCKET

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-bucket:ab9b84afef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-2034712ffe) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5fe6983476"></a>
| Field | Shape |
|---|---|
| <a id="s-939e46bead"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-4e50679c67"></a>`default_expressions` | ["''"] |
| <a id="s-46a05f24bc"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_BUCKET" |
| <a id="s-0ce1d91389"></a>`input_shape` | "environment-string" |
| <a id="s-f7d158a894"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_BUCKET" |
| <a id="s-0050bf8e14"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-6d50f59a49"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_BUCKET](../../../evidence/sources.md#src-363b39cd6e) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/88`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 904638ba52a13d3492a193e1af1dd316aaa5fcfafb253e5fd49fcca5faa37eb1 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_BUCKET",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_BUCKET",
  "owner": "riverhog-storage-adapter-aws"
}
```
