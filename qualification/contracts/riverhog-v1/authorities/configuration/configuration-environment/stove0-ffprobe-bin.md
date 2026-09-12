# STOVE0_FFPROBE_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-bin:dbc89eb4d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6e5c2a5e0084"></a>
| Field | Shape |
|---|---|
| <a id="s-8adc3e5c2206"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-bc8e0830d2fa"></a>`name` | "STOVE0_FFPROBE_BIN" |

## Governing policies

- <a id="pa-8dcd912f4ab1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_BIN](../../../evidence/sources.md#src-2599a68a211b) — `configuration-environment:STOVE0_FFPROBE_BIN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/96`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ae2ccba2e7de1e9ab168ac453e13e1e8e6961c6de5832a8b92bbae8507f2213 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_BIN"
}
```
