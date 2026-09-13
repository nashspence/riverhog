# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-base-url:6228e2e81f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-11a73a67cc"></a>
| Field | Shape |
|---|---|
| <a id="s-56b56d915a"></a>`consumers` | ["riverhog-client"] |
| <a id="s-2837f1949b"></a>`default_expressions` | ["unset"] |
| <a id="s-05a1ce1ce4"></a>`id` | "riverhog-client:environment:RIVERHOG_BASE_URL" |
| <a id="s-a39bc19e88"></a>`input_shape` | "environment-string" |
| <a id="s-514e7dfc94"></a>`name` | "RIVERHOG_BASE_URL" |
| <a id="s-bba7a7ca74"></a>`owner` | "riverhog-client" |

## Governing policies

- <a id="pa-28f552d6db"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_BASE_URL](../../../evidence/sources.md#src-475671fe74) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv('RIVERHOG_BASE_URL')` |

### Machine authority

- `/external_contract/configuration_environment/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02c74db099381f19d65370253d80f4ae683173d74e5dcff0a7c9a80bc9cf51b9 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-client:environment:RIVERHOG_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BASE_URL",
  "owner": "riverhog-client"
}
```
