# RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-scrypt-work-factor:7ccbf81a74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ee8d4c5bb2"></a>
| Field | Shape |
|---|---|
| <a id="s-26db15b0c1"></a>`classification` | "runtime" |
| <a id="s-9cef198e9c"></a>`consumers` | ["riverhog-server"] |
| <a id="s-ef178cc569"></a>`disposition` | "contractual" |
| <a id="s-c87995acf3"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR" |
| <a id="s-199cea8e22"></a>`name` | "RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR" |
| <a id="s-a35e48e93a"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-acd7c7f970"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR](../../../evidence/sources.md#src-3434cbb4f1) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `_parse_int(os.getenv('RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR', str(DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR)), name='RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR', minimum=1)` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR', str(DEFAULT_ARCHIVE_SCRYPT_WORK_FACTOR))` |

### Machine authority

- `/external_contract/configuration_environment/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d5bafe6d9f0966de1111e1f25d1807857786bc235e9f210750e9f238197bd59 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR",
  "name": "RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR",
  "owner": "riverhog-server"
}
```
