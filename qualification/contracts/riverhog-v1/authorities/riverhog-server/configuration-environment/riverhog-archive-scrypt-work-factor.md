# RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-scrypt-work-factor:b590ffce21 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-dac516bc47"></a>

| Field | Value |
|---|---|
| <a id="s-457ed73ac2"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-63608cfcca"></a>`default_expressions` | `["str(DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR)"]` |
| <a id="s-26cf215710"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR"` |
| <a id="s-c508465db1"></a>`input_shape` | `"environment-string"` |
| <a id="s-9c80ef951b"></a>`name` | `"RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR"` |
| <a id="s-946b8d2b86"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-1d8346f44b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR](../../../evidence/sources/authorities.md#src-3434cbb4f1) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR', str(DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR))` |

### Machine authority

- `/external_contract/configuration_environment/176`

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
