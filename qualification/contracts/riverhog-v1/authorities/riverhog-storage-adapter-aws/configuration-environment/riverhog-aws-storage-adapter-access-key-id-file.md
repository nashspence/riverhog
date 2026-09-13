# RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-access-key-id-file:a8b39e5902 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-284bbb4914"></a>
| Field | Shape |
|---|---|
| <a id="s-c9aa0936f8"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-bb482cec50"></a>`default_expressions` | ["unset"] |
| <a id="s-f91da4b941"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE" |
| <a id="s-f863ec0947"></a>`input_shape` | "environment-string" |
| <a id="s-9f2399bb48"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE" |
| <a id="s-1932bf0a08"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-217da38b92"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE](../../../evidence/sources.md#src-9502ea113d) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/86`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a25228786c266e637605d74089abc72e1a3c25e5dc01fdf0ba48cbb6c0a59ee6 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE",
  "owner": "riverhog-storage-adapter-aws"
}
```
