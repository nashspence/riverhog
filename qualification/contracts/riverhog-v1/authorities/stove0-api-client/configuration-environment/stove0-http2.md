# STOVE0_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-http2:49a56d3059 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4abc02115b"></a>
| Field | Shape |
|---|---|
| <a id="s-4e0a108175"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-2d82ee102a"></a>`default_expressions` | ["unset"] |
| <a id="s-f2e09fd5e9"></a>`id` | "stove0-api-client:environment:STOVE0_HTTP2" |
| <a id="s-40ccec5f8c"></a>`input_shape` | "environment-string" |
| <a id="s-ca538f225d"></a>`name` | "STOVE0_HTTP2" |
| <a id="s-79625ca4c2"></a>`owner` | "stove0-api-client" |

## Governing policies

- <a id="pa-04571d2825"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-api-client:STOVE0_HTTP2](../../../evidence/sources.md#src-29a943f529) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/145`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dbecdef2c4cfee74ba9f3643baeabafd24d4e036694a597ff8f3b2685e97b21f -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-api-client:environment:STOVE0_HTTP2",
  "input_shape": "environment-string",
  "name": "STOVE0_HTTP2",
  "owner": "stove0-api-client"
}
```
