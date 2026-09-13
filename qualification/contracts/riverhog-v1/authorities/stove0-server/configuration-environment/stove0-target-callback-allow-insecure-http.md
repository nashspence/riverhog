# STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-callback-allow-insecure-http:07abc5d1fb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-511d2ba0da"></a>
| Field | Shape |
|---|---|
| <a id="s-95f3a14158"></a>`consumers` | ["stove0-server"] |
| <a id="s-3f7daf7f5f"></a>`default_expressions` | ["unset"] |
| <a id="s-eefa9e99c9"></a>`id` | "stove0-server:environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP" |
| <a id="s-168f3b71fe"></a>`input_shape` | "environment-string" |
| <a id="s-f58b650a7a"></a>`name` | "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP" |
| <a id="s-13d61e494d"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-cb390e38f9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-00eaae2681) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/244`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7279023a5dfc3f21ce245c451d9d61bdfe9e3536012320f529f151833635ff78 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-server:environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP",
  "owner": "stove0-server"
}
```
