# RIVERHOG_AWS_STORAGE_ADAPTER_TCP_KEEPALIVE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-tcp-keepalive:5725eb96bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-a5bdad2233"></a>

| Field | Value |
|---|---|
| <a id="s-ee93841a02"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-27341a9850"></a>`default_expressions` | `["''"]` |
| <a id="s-1eb66f737d"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_TCP_KEEPALIVE"` |
| <a id="s-6e5299b395"></a>`input_shape` | `"environment-string"` |
| <a id="s-4ca352f816"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_TCP_KEEPALIVE"` |
| <a id="s-2bd29e5838"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-a91e5e213b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_TCP_KEEPALIVE](../../../evidence/sources.md#src-149ae68f5d) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py::\_optional](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/112`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29ee4257925eef6fef92cdb6041a7ea353ffdbfa5d1b26d74bdfd25a41d68e6d -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_TCP_KEEPALIVE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_TCP_KEEPALIVE",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
