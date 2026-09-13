# STOVE0_TARGET_CALLBACK_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-callback-base-url:84902bf9c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-12475197c9) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ce7b3c0fe5"></a>
| Field | Shape |
|---|---|
| <a id="s-b531aaadad"></a>`consumers` | ["stove0-server"] |
| <a id="s-0b0de17673"></a>`default_expressions` | ["''"] |
| <a id="s-39fe167954"></a>`id` | "stove0-server:environment:STOVE0_TARGET_CALLBACK_BASE_URL" |
| <a id="s-dab7acf396"></a>`input_shape` | "environment-string" |
| <a id="s-92dba1f18c"></a>`name` | "STOVE0_TARGET_CALLBACK_BASE_URL" |
| <a id="s-5460aac20b"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-b426c1ddac"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_BASE_URL](../../../evidence/sources.md#src-074108cdde) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get('STOVE0_TARGET_CALLBACK_BASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/245`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f53cd051a0317923692e35edfd377327ef5e230ce56633ff7a98a5f614449ab -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_TARGET_CALLBACK_BASE_URL",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGET_CALLBACK_BASE_URL",
  "owner": "stove0-server"
}
```
