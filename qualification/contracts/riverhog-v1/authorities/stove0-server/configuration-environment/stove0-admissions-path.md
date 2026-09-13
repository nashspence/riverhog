# STOVE0_ADMISSIONS_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-admissions-path:f5bc1b5091 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f82c408db3"></a>
| Field | Shape |
|---|---|
| <a id="s-92991d302e"></a>`consumers` | ["stove0-server"] |
| <a id="s-e8c8558325"></a>`default_expressions` | ["''"] |
| <a id="s-6686ac562c"></a>`id` | "stove0-server:environment:STOVE0_ADMISSIONS_PATH" |
| <a id="s-a1d5fd2666"></a>`input_shape` | "environment-string" |
| <a id="s-dbdacef493"></a>`name` | "STOVE0_ADMISSIONS_PATH" |
| <a id="s-41ea2c0785"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-47ce18c727"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_ADMISSIONS_PATH](../../../evidence/sources.md#src-3e52273877) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get('STOVE0_ADMISSIONS_PATH', '')` |

### Machine authority

- `/external_contract/configuration_environment/228`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acb1d11e5c92f10fd750358fd57a2c1cf682d5407353ef7bea6328bb129544dd -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_ADMISSIONS_PATH",
  "input_shape": "environment-string",
  "name": "STOVE0_ADMISSIONS_PATH",
  "owner": "stove0-server"
}
```
