# STOVE0_OPUS_TARGET_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-state-root:b6ac6d62cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-b0c1830930) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1d721b1c20"></a>
| Field | Shape |
|---|---|
| <a id="s-942059049f"></a>`consumers` | ["stove0-opus-target"] |
| <a id="s-0448dc0991"></a>`default_expressions` | ["'/var/lib/stove0-opus-target'"] |
| <a id="s-0e6d248bf5"></a>`id` | "stove0-opus-target:environment:STOVE0_OPUS_TARGET_STATE_ROOT" |
| <a id="s-bb2f376cde"></a>`input_shape` | "environment-string" |
| <a id="s-9e2351c49c"></a>`name` | "STOVE0_OPUS_TARGET_STATE_ROOT" |
| <a id="s-211e3320b0"></a>`owner` | "stove0-opus-target" |

## Governing policies

- <a id="pa-596517e553"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_STATE_ROOT](../../../evidence/sources.md#src-ba7aa199b0) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv(f'{prefix}_STATE_ROOT', '/var/lib/stove0-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/195`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1bf86daf99724cb8dc4a3850ac99ad99e4181145ec4a1337cac1ad70f277446d -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "'/var/lib/stove0-opus-target'"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_STATE_ROOT",
  "owner": "stove0-opus-target"
}
```
