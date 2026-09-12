# RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-ingress-max-inflight-bytes:3ae8a04bc4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-96459587f4"></a>
| Field | Shape |
|---|---|
| <a id="s-02ba303908"></a>`classification` | "runtime" |
| <a id="s-77d8284e62"></a>`consumers` | ["riverhog-server"] |
| <a id="s-6eb233c2fd"></a>`disposition` | "contractual" |
| <a id="s-db2f70ffc3"></a>`id` | "riverhog-server:environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES" |
| <a id="s-a4e0febe13"></a>`name` | "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES" |
| <a id="s-efcd10c3b8"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](#s-96459587f4) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-093545536c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-0d3a71a551"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../../../evidence/sources.md#src-e1d4295958) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_bytes(values, 'RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES', DEFAULT_UPLOAD_MAX_INFLIGHT_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/56`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b43d1123edbc2423e209217034826c02c138f48aa8e72a9c0f2dc18beb88f8cf -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES",
  "name": "RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES",
  "owner": "riverhog-server"
}
```
