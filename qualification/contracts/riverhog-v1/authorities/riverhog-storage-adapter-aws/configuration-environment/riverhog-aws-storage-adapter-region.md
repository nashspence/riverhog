# RIVERHOG_AWS_STORAGE_ADAPTER_REGION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-region:585728c565 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9f9eafd5d5"></a>

| Field | Value |
|---|---|
| <a id="s-8248147251"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-be154e911d"></a>`default_expressions` | `["''"]` |
| <a id="s-8211605975"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_REGION"` |
| <a id="s-c550564cdf"></a>`input_shape` | `"environment-string"` |
| <a id="s-0e24792ee3"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_REGION"` |
| <a id="s-48214e7d5d"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-1a97dd2f77"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_REGION](../../../evidence/sources.md#src-68cd9cdbbc) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py::\_required](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py) | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/103`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4740d652ff0a8df6a32592e15fdec335493195306af93fd6c0802c2fff4040cc -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_REGION",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_REGION",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
