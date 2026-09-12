# STOVE0_EXIFTOOL_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-host:93ba793080 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5fe698347649"></a>
| Field | Shape |
|---|---|
| <a id="s-939e46bead72"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-f7d158a894b3"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_HOST" |

## Governing policies

- <a id="pa-a59e6b56ccb5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST](../../../evidence/sources.md#src-ca32efa9d753) — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_HOST`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/88`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f66d309ba0cb05a29879046d2b6d83145062523048e203034d0e9efbeb3c9c7 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_HOST"
}
```
