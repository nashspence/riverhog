# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-token-file:375fce7bbf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-663ea2a8c925"></a>
| Field | Shape |
|---|---|
| <a id="s-e37a73aa7abd"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-f850425dcc72"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE" |

## Governing policies

- <a id="pa-bf5c5596523b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE](../../../evidence/sources.md#src-dc1fbdf12277) — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/102`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68ff9f5ee242a4704edf78a7ce1c3afa36b416e9bdd880bb4fc4abd20d62374e -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE"
}
```
