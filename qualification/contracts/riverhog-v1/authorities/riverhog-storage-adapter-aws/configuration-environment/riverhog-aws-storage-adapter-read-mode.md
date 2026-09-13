# RIVERHOG_AWS_STORAGE_ADAPTER_READ_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-read-mode:a1484aac99 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-2034712ffe) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eb8f28adc2"></a>
| Field | Shape |
|---|---|
| <a id="s-f44dec2d22"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-73dce300f6"></a>`default_expressions` | ["''"] |
| <a id="s-bf6317e26b"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_READ_MODE" |
| <a id="s-a9ad8d226b"></a>`input_shape` | "environment-string" |
| <a id="s-c875192905"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_READ_MODE" |
| <a id="s-9e801618d0"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-991d2c0e72"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_MODE](../../../evidence/sources.md#src-45d661871e) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/101`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92e7ea74e38b32c511bb3bebedc1e1ec9ca90f03b9678aa4e5155c6a971ec916 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_READ_MODE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_READ_MODE",
  "owner": "riverhog-storage-adapter-aws"
}
```
