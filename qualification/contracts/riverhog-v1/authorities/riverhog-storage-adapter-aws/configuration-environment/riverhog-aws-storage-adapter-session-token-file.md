# RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-session-token-file:93ea5bbf32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7baa03212b"></a>

| Field | Value |
|---|---|
| <a id="s-8d23c42891"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-c2f443c58f"></a>`default_expressions` | `["unset"]` |
| <a id="s-0da1805b51"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN_FILE"` |
| <a id="s-abd8a91df5"></a>`input_shape` | `"environment-string"` |
| <a id="s-aa9b56325e"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN_FILE"` |
| <a id="s-5216e992db"></a>`owner` | `"riverhog-storage-adapter-aws"` |

## Governing policies

- <a id="pa-682fa6d61f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN_FILE](../../../evidence/sources/authorities.md#src-e8515dc45f) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py::\_optional\_secret](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/111`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b27edae9ce290d57076392252c254ef2aaaa31219aaf4615d2dd9072b70efa1 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN_FILE",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
