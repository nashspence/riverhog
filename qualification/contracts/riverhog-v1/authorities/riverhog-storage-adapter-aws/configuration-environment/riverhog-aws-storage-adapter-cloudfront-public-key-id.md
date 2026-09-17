# RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PUBLIC_KEY_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-cloudfront-p-7d547c77f6:bfd9f782d6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-74f1ce0aa1"></a>

| Field | Value |
|---|---|
| <a id="s-14c637bf7d"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-438a310e71"></a>`default_expressions` | `["''"]` |
| <a id="s-5670701bbc"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PUBLIC_KEY_ID"` |
| <a id="s-5218f9ba8a"></a>`input_shape` | `"environment-string"` |
| <a id="s-9911339263"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PUBLIC_KEY_ID"` |
| <a id="s-a4a31c47b8"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-c2356b2e99"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PUBLIC_KEY_ID](../../../evidence/sources/authorities.md#src-1bbc37d425) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py::\_optional](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/91`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 159b7203218a9df92436417ec0a6739f65aa55e5dd4b36c47b43ad74a6ffaa29 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PUBLIC_KEY_ID",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PUBLIC_KEY_ID",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
