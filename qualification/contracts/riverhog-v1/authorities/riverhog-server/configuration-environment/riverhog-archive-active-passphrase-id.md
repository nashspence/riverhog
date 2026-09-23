# RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-active-passphrase-id:ce46300325 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-60d36f56ec"></a>

| Field | Value |
|---|---|
| <a id="s-6a75627f71"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-7f0cb5f08e"></a>`default_expressions` | `["''"]` |
| <a id="s-9855b52610"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID"` |
| <a id="s-93a8c602ad"></a>`input_shape` | `"environment-string"` |
| <a id="s-25d67a7319"></a>`name` | `"RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID"` |
| <a id="s-1c9291919b"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-ccc776d9d4"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID](../../../evidence/sources/authorities.md#src-59ce1b7699) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID', '')` |

### Machine authority

- `/external_contract/configuration_environment/171`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d0eef78a4476706a6cea21a19d50dd065e64dfee3dd42da86992b49a25d8a8d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID",
  "owner": "riverhog-server"
}
```

</details>
