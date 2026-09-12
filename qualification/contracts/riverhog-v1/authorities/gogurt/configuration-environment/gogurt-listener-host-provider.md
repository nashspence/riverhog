# GOGURT_LISTENER_HOST_PROVIDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:gogurt:gogurt-listener-host-provider:82c5cecfe2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-74d40ab69d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7ba4daea68"></a>
| Field | Shape |
|---|---|
| <a id="s-a6a9ee838f"></a>`classification` | "identity" |
| <a id="s-1fb6988e81"></a>`consumers` | ["gogurt"] |
| <a id="s-439a939eed"></a>`disposition` | "contractual" |
| <a id="s-580dfdcf0a"></a>`id` | "gogurt:environment:GOGURT_LISTENER_HOST_PROVIDER" |
| <a id="s-0df8f11eac"></a>`name` | "GOGURT_LISTENER_HOST_PROVIDER" |
| <a id="s-f3d50fff16"></a>`owner` | "gogurt" |

## Governing policies

- <a id="pa-147b7d4e5a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:gogurt:GOGURT_LISTENER_HOST_PROVIDER](../../../evidence/sources.md#src-a33048bae0) — `reference/gogurt/application/src/gogurt/cli.py`
- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/0/names` |
| parser | `gogurt` | `reference/gogurt/application/src/gogurt/cli.py` | `typer.Option('--listener-host-provider', envvar='GOGURT_LISTENER_HOST_PROVIDER', help='Exact installed listener-host provider name.')` |

### Machine authority

- `/external_contract/configuration_environment/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 638b36a396c9f93c588eac3b2c09a9fc8090f4296a74318327dbf672ba80b89e -->

```json
{
  "classification": "identity",
  "consumers": [
    "gogurt"
  ],
  "disposition": "contractual",
  "id": "gogurt:environment:GOGURT_LISTENER_HOST_PROVIDER",
  "name": "GOGURT_LISTENER_HOST_PROVIDER",
  "owner": "gogurt"
}
```
