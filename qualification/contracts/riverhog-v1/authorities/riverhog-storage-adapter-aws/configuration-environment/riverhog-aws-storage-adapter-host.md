# RIVERHOG_AWS_STORAGE_ADAPTER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-host:c04ee218e5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d15fb4b928"></a>
| Field | Shape |
|---|---|
| <a id="s-98b9d4f5c3"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-a6c332bf33"></a>`default_expressions` | ["'127.0.0.1'"] |
| <a id="s-7c79245e59"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_HOST" |
| <a id="s-32b8dc3225"></a>`input_shape` | "environment-string" |
| <a id="s-a54012d8c0"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_HOST" |
| <a id="s-fe903fc9c1"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-da0a509187"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_HOST](../../../evidence/sources.md#src-bb7f6a2fb9) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/95`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 633be73e0fa2c9711f1a6f3c2100586abc18dac08d3df0f94b837192d5fd13d3 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_HOST",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_HOST",
  "owner": "riverhog-storage-adapter-aws"
}
```
