# RIVERHOG_ARCHIVE_WRITE_STORE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-write-store:883509b2e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5581cd63e1"></a>

| Field | Value |
|---|---|
| <a id="s-77d05a28fd"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-64e0770d83"></a>`default_expressions` | `["names[0]"]` |
| <a id="s-f39456d089"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_STORE"` |
| <a id="s-d4011511cf"></a>`input_shape` | `"environment-string"` |
| <a id="s-f0549843d7"></a>`name` | `"RIVERHOG_ARCHIVE_WRITE_STORE"` |
| <a id="s-4055a76d48"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-ca2580ea94"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_STORE](../../../evidence/sources.md#src-2a3a35733e) — [riverhog/src/riverhog\_core/runtime\_config.py::\_parse\_archive\_stores](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `values.get('RIVERHOG_ARCHIVE_WRITE_STORE', names[0])` |

### Machine authority

- `/external_contract/configuration_environment/47`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 958af2ef0da0f760e1db651d28221a9a3e9e89b64802d9dde65106e1c052077b -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "names[0]"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_STORE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_WRITE_STORE",
  "owner": "riverhog-server"
}
```

</details>
