# RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-session-token:c37cbc8e3a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6cd1741870"></a>
| Field | Shape |
|---|---|
| <a id="s-f89c1c3048"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-4209677621"></a>`default_expressions` | ["unset"] |
| <a id="s-ee9ed3533d"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN" |
| <a id="s-42c7b7c437"></a>`input_shape` | "environment-string" |
| <a id="s-7027ddbe71"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN" |
| <a id="s-5b4c4db475"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-6e738eb74f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN](../../../evidence/sources.md#src-74ac0a4c8b) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.environ.pop(direct_name)` |
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/110`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7afa73a4e0f7bf6e0f8cc6bb0ba5a00f3f4690c1535a2a0825f7a18c3ed64e23 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN",
  "owner": "riverhog-storage-adapter-aws"
}
```
