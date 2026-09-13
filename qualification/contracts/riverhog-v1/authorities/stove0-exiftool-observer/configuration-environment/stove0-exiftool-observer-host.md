# STOVE0_EXIFTOOL_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-exiftool-observer:stove0-exiftool-observer-host:1eb40c9913 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6b9a1cd0e6"></a>
| Field | Shape |
|---|---|
| <a id="s-a1ede7b4cd"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-0e927dd781"></a>`default_expressions` | ["'127.0.0.1'"] |
| <a id="s-eb324e3723"></a>`id` | "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_HOST" |
| <a id="s-0147bb6300"></a>`input_shape` | "environment-string" |
| <a id="s-b2c273c0ba"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_HOST" |
| <a id="s-4e30a4d523"></a>`owner` | "stove0-exiftool-observer" |

## Governing policies

- <a id="pa-5c6b49a56e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_HOST](../../../evidence/sources.md#src-8dfcd7e460) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-exiftool-observer` | `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/app.py` | `os.getenv('STOVE0_EXIFTOOL_OBSERVER_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/149`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d0d2b0ad62550f7ad32b44adeb0786fc39c1dcecab73981e1119b82325e0022 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-exiftool-observer:environment:STOVE0_EXIFTOOL_OBSERVER_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_EXIFTOOL_OBSERVER_HOST",
  "owner": "stove0-exiftool-observer"
}
```
