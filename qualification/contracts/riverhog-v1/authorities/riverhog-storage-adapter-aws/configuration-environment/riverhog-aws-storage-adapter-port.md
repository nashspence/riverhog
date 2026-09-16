# RIVERHOG_AWS_STORAGE_ADAPTER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-port:224fa0e94c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-551254e2cb"></a>

| Field | Value |
|---|---|
| <a id="s-27a1ad1a86"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-dd3a6cbf0c"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-3a0a03e6eb"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_PORT"` |
| <a id="s-21fd72eb86"></a>`input_shape` | `"environment-string"` |
| <a id="s-fd2ae4a599"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_PORT"` |
| <a id="s-50b4a1ec36"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-d7e52b3c20"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_PORT](../../../evidence/sources.md#src-75dcbe865e) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/99`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e66ee1b13347325bfbdafc099d3938c5d6a9c93bd2922d77e213aac69d45f14 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_PORT",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_PORT",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
