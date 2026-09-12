# STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-source-revision:3e84450e57 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-960cd64e68"></a>
| Field | Shape |
|---|---|
| <a id="s-194d4c0b99"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-fc06ab9bf9"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION" |

## Governing policies

- <a id="pa-beb26ad89c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION](../../../evidence/sources.md#src-5e0f54d657) — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/100`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 067b79778e45e1bcebf559f08505f8bee3079c89a99d4e24eff39f31bf1223f2 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION"
}
```
