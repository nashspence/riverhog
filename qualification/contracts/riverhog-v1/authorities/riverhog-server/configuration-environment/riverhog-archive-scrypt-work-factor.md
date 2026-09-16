# RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-scrypt-work-factor:92e0418b6a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ba8bf00da0"></a>

| Field | Value |
|---|---|
| <a id="s-fefed5a14e"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-9f9519b503"></a>`default_expressions` | `["str(DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR)"]` |
| <a id="s-6e9078eeef"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR"` |
| <a id="s-b017fa7a37"></a>`input_shape` | `"environment-string"` |
| <a id="s-5c224fe460"></a>`name` | `"RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR"` |
| <a id="s-316a058fc1"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-9485ecb079"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR](../../../evidence/sources.md#src-3434cbb4f1) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR', str(DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR))` |

### Machine authority

- `/external_contract/configuration_environment/42`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe7aac0e70cdf0521b865961eb2c90eff76f60a7b32c8be97053597ef7214734 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "str(DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR)"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR",
  "owner": "riverhog-server"
}
```

</details>
