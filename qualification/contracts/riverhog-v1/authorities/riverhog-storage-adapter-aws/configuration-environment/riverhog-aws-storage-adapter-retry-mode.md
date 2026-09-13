# RIVERHOG_AWS_STORAGE_ADAPTER_RETRY_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-retry-mode:47e39383bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-62bc3f7cc9"></a>
| Field | Shape |
|---|---|
| <a id="s-119f21dafe"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-3c87eac384"></a>`default_expressions` | ["''"] |
| <a id="s-01069a2d81"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_RETRY_MODE" |
| <a id="s-a900a6f3a0"></a>`input_shape` | "environment-string" |
| <a id="s-02fadb9af0"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_RETRY_MODE" |
| <a id="s-6ab5fa12ac"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-8da8f00662"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_RETRY_MODE](../../../evidence/sources.md#src-8b8b0e39d8) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/106`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 641f686051884c66883dd34888da07035e3499a736336a9648d90656da8cc791 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_RETRY_MODE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_RETRY_MODE",
  "owner": "riverhog-storage-adapter-aws"
}
```
