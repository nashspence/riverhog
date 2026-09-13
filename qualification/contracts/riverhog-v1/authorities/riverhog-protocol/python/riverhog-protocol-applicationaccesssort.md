# riverhog_protocol.ApplicationAccessSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-applicationaccesssort:2177b65793 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0834a1c25e"></a>
| Field | Shape |
|---|---|
| <a id="s-9e0552d546"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-e577240a80"></a>`distribution` | "riverhog-protocol" |
| <a id="s-2195ddf166"></a>`module` | "riverhog_protocol" |
| <a id="s-6e9e00b3bb"></a>`name` | "ApplicationAccessSort" |
| <a id="s-ccce620cba"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2049ca408d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ApplicationAccessSort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c83c3375d0ce7ae2855963d470479849751ea07ed28638d14717d1a789dd15ec -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['app', 'key_id', 'permission', 'resource', 'created_at']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ApplicationAccessSort",
  "unit": "export"
}
```
