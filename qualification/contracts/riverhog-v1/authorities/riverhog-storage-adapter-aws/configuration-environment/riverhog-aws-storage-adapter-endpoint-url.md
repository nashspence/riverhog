# RIVERHOG_AWS_STORAGE_ADAPTER_ENDPOINT_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-endpoint-url:eaf58b476f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-2034712ffe) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1513c088ba"></a>
| Field | Shape |
|---|---|
| <a id="s-9d88b5d281"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-bb088213c3"></a>`default_expressions` | ["''"] |
| <a id="s-eeebfb92ac"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ENDPOINT_URL" |
| <a id="s-f5438b1f3d"></a>`input_shape` | "environment-string" |
| <a id="s-66c149272c"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_ENDPOINT_URL" |
| <a id="s-f9c1542d89"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-638611b891"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ENDPOINT_URL](../../../evidence/sources.md#src-88014cadc1) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/93`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8bdfca4e69a2990fde85140265e9307d6a0c50cef6d282290fe4eb6d3106698 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_ENDPOINT_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_ENDPOINT_URL",
  "owner": "riverhog-storage-adapter-aws"
}
```
