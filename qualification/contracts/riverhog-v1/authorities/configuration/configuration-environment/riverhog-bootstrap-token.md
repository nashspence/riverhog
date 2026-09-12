# RIVERHOG_BOOTSTRAP_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-bootstrap-token:5ed79e01de -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-da42edbdaef9"></a>
| Field | Shape |
|---|---|
| <a id="s-cb8b3d3b8e7b"></a>`consumers` | ["riverhog-server"] |
| <a id="s-c08568c77afa"></a>`name` | "RIVERHOG_BOOTSTRAP_TOKEN" |

## Governing policies

- <a id="pa-a14cb2ce64cb"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN](../../../evidence/sources.md#src-914d97b76beb) — `configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/24`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc21aa0955fbe979d3969c62040325732a936d49d02cbca4ea6b687a8d4c8bc6 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_BOOTSTRAP_TOKEN"
}
```
