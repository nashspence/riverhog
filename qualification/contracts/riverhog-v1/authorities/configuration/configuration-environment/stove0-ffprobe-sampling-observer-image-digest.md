# STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-image-digest:b7413a1174 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b3dd205ff4a6"></a>
| Field | Shape |
|---|---|
| <a id="s-fdfa36834ac0"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-7a20528c0bee"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST" |

## Governing policies

- <a id="pa-ae2008a3dd4a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST](../../../evidence/sources.md#src-0ec6f3c354a6) — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/98`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 468a7857495b1d25cc9b0b8491ab01aad992c987d960c91703026792f84bcab4 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST"
}
```
