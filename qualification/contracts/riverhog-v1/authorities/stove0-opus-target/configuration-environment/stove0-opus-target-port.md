# STOVE0_OPUS_TARGET_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-port:d99a66c856 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-40c19ca11c"></a>
| Field | Shape |
|---|---|
| <a id="s-123b5940ee"></a>`consumers` | ["stove0-opus-target"] |
| <a id="s-afd20a934b"></a>`default_expressions` | ["'8080'"] |
| <a id="s-6d636f42e0"></a>`id` | "stove0-opus-target:environment:STOVE0_OPUS_TARGET_PORT" |
| <a id="s-c5c8a72035"></a>`input_shape` | "environment-string" |
| <a id="s-eb404246aa"></a>`name` | "STOVE0_OPUS_TARGET_PORT" |
| <a id="s-ff9cf98d21"></a>`owner` | "stove0-opus-target" |

## Governing policies

- <a id="pa-7edf8c127a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_PORT](../../../evidence/sources.md#src-9e592324a6) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/193`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e7fbf91df0c36653dccf7446a5014d3595b905fed5a9647fd67c46636bd5993 -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_PORT",
  "owner": "stove0-opus-target"
}
```
