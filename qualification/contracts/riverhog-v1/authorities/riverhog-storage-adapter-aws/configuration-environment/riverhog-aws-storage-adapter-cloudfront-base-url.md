# RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-cloudfront-base-url:45b0c81e41 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-2034712ffe) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3a00485a0d"></a>
| Field | Shape |
|---|---|
| <a id="s-f4162f50d7"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-64cdad0f75"></a>`default_expressions` | ["''"] |
| <a id="s-c084c603a4"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_BASE_URL" |
| <a id="s-198c63854c"></a>`input_shape` | "environment-string" |
| <a id="s-811edc1b84"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_BASE_URL" |
| <a id="s-a77e047c01"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-05aff58664"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_BASE_URL](../../../evidence/sources.md#src-244cb79acb) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/89`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e34f9887e0a850296b2bcddef5cb3d642cec2b48e6e1d0c37a1e38316c110cb9 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_BASE_URL",
  "owner": "riverhog-storage-adapter-aws"
}
```
