# RIVERHOG_LOG_LEVEL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-log-level:dc7b9c626b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9857458ffa"></a>
| Field | Shape |
|---|---|
| <a id="s-fe693377ae"></a>`consumers` | ["riverhog-server"] |
| <a id="s-03c589148c"></a>`name` | "RIVERHOG_LOG_LEVEL" |

## Governing policies

- <a id="pa-31dc138ea0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_LOG_LEVEL](../../../evidence/sources.md#src-180b30299f) — `configuration-environment:RIVERHOG_LOG_LEVEL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/51`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43687f68407eb01083f0d7e98e6f52a4e8650852fdfe452cb07dc905ab208814 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_LOG_LEVEL"
}
```
