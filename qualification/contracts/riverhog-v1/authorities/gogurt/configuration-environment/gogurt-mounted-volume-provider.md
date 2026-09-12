# GOGURT_MOUNTED_VOLUME_PROVIDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:gogurt:gogurt-mounted-volume-provider:f92792f700 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-74d40ab69d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0d1f6f2eed"></a>
| Field | Shape |
|---|---|
| <a id="s-f32ebd402f"></a>`classification` | "identity" |
| <a id="s-1351b3e99a"></a>`consumers` | ["gogurt"] |
| <a id="s-b13b1c5cc1"></a>`disposition` | "contractual" |
| <a id="s-8efd3e0ae2"></a>`id` | "gogurt:environment:GOGURT_MOUNTED_VOLUME_PROVIDER" |
| <a id="s-4e078e6d98"></a>`name` | "GOGURT_MOUNTED_VOLUME_PROVIDER" |
| <a id="s-5f4b8348df"></a>`owner` | "gogurt" |

## Governing policies

- <a id="pa-6bb234dab7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:gogurt:GOGURT_MOUNTED_VOLUME_PROVIDER](../../../evidence/sources.md#src-e97a418da2) — `reference/gogurt/application/src/gogurt/cli.py`
- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/0/names` |
| parser | `gogurt` | `reference/gogurt/application/src/gogurt/cli.py` | `typer.Option('--mounted-volume-provider', envvar='GOGURT_MOUNTED_VOLUME_PROVIDER', help='Exact installed mounted-volume provider name.')` |

### Machine authority

- `/external_contract/configuration_environment/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ff72c13cda466077738009b916776072c9fba6ebe02dd9b1b7830fc1a7683ab -->

```json
{
  "classification": "identity",
  "consumers": [
    "gogurt"
  ],
  "disposition": "contractual",
  "id": "gogurt:environment:GOGURT_MOUNTED_VOLUME_PROVIDER",
  "name": "GOGURT_MOUNTED_VOLUME_PROVIDER",
  "owner": "gogurt"
}
```
