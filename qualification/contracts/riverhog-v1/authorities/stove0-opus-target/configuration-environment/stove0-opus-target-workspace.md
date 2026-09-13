# STOVE0_OPUS_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-workspace:764b29bea1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-b0c1830930) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e9a01debe6"></a>
| Field | Shape |
|---|---|
| <a id="s-ebb317ac70"></a>`consumers` | ["stove0-opus-target"] |
| <a id="s-68cbb4f4c7"></a>`default_expressions` | ["'/run/stove0-opus-target'"] |
| <a id="s-bd4e6d9d2a"></a>`id` | "stove0-opus-target:environment:STOVE0_OPUS_TARGET_WORKSPACE" |
| <a id="s-07cd635e83"></a>`input_shape` | "environment-string" |
| <a id="s-0b20ca18fe"></a>`name` | "STOVE0_OPUS_TARGET_WORKSPACE" |
| <a id="s-a3279fc613"></a>`owner` | "stove0-opus-target" |

## Governing policies

- <a id="pa-637648f2ab"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_WORKSPACE](../../../evidence/sources.md#src-d621aac3e2) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv(f'{prefix}_WORKSPACE', '/run/stove0-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/198`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4fba676e5e850758a47148f388a7799f6a56542d5ed3f3917fd58cc1964f23fe -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "'/run/stove0-opus-target'"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_WORKSPACE",
  "owner": "stove0-opus-target"
}
```
