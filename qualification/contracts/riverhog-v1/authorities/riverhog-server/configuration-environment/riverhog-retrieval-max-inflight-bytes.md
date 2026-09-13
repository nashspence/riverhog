# RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-inflight-bytes:bf3964bcd9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-0e8ce1fd7c"></a>
| Field | Shape |
|---|---|
| <a id="s-8e543c475e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-cfec5ebc31"></a>`default_expressions` | ["unset"] |
| <a id="s-c7d082cc1f"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES" |
| <a id="s-8dde74de5f"></a>`input_shape` | "environment-string" |
| <a id="s-38ce56b288"></a>`name` | "RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES" |
| <a id="s-efc59f7a1f"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](#s-0e8ce1fd7c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-37ad17bff7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-219de31745"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../../../evidence/sources.md#src-cb29acf59b) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/76`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0cfc26586e996aee347209ec395d5a37b9c4ca31deb33c975ea38f387b81c7b -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES",
  "owner": "riverhog-server"
}
```
