# RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-secret-access-key:88ec962294 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8af4063a2b"></a>
| Field | Shape |
|---|---|
| <a id="s-780fea1d0c"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-81227f6e06"></a>`default_expressions` | ["unset"] |
| <a id="s-2760bd28e2"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY" |
| <a id="s-4a2f875513"></a>`input_shape` | "environment-string" |
| <a id="s-9feb87a0c4"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY" |
| <a id="s-095861fb48"></a>`owner` | "riverhog-storage-adapter-aws" |

## Governing policies

- <a id="pa-70648e80ef"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY](../../../evidence/sources.md#src-6b5c5953fa) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.environ.pop(direct_name)` |
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/108`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37657e1cf294f5333a684ddaedf3fa370701c1ca3c533e10030aad4d33988dbe -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY",
  "owner": "riverhog-storage-adapter-aws"
}
```
