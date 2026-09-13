# STOVE0_TARGETS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-targets-json:fe3a69a6d9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-12475197c9) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-704a0f2188"></a>
| Field | Shape |
|---|---|
| <a id="s-a976fdbf26"></a>`consumers` | ["stove0-server"] |
| <a id="s-c44939a0ce"></a>`default_expressions` | ["'{}'"] |
| <a id="s-cf1d980c19"></a>`id` | "stove0-server:environment:STOVE0_TARGETS_JSON" |
| <a id="s-80e58005d1"></a>`input_shape` | "environment-string" |
| <a id="s-6ced135eba"></a>`name` | "STOVE0_TARGETS_JSON" |
| <a id="s-8c5a0f1546"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-184b4a9ea3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGETS_JSON](../../../evidence/sources.md#src-05b1eb365e) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, '{}')` |

### Machine authority

- `/external_contract/configuration_environment/242`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5078f653c930d933f6878a54ac017de18603b98b5d663fa18e4b13c9ec60dd6d -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "'{}'"
  ],
  "id": "stove0-server:environment:STOVE0_TARGETS_JSON",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGETS_JSON",
  "owner": "stove0-server"
}
```
