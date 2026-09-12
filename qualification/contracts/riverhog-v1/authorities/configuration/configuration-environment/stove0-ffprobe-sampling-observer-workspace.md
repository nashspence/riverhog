# STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-workspace:e9b8df9d92 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9f9eafd5d515"></a>
| Field | Shape |
|---|---|
| <a id="s-824814725114"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-0e24792ee3df"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE" |

## Governing policies

- <a id="pa-74028f6f7fc4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE](../../../evidence/sources.md#src-2c94d247c4a3) — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/103`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c301b6b4fcc48e78c0ead5cf7db7769735e0fbf678d702bf701a23408a88ff83 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE"
}
```
