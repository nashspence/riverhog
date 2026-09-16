# riverhog_protocol.ProcessingClaimFenceDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimfencedocument-get:3184214759 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-400afc94d4"></a>
- <a id="s-78516bc791"></a>`distribution`: `riverhog-protocol`
- <a id="s-9c19567d20"></a>`module`: `riverhog_protocol`
- <a id="s-25570ec8ec"></a>`name`: `get`
- <a id="s-8b40c2caae"></a>`owner`: `riverhog_protocol.ProcessingClaimFenceDocument`
- <a id="s-1b81e461c4"></a>`unit`: `member`

### Declared structure

- <a id="s-3336e9b7e3"></a>`kind`: `"method"`
- <a id="s-655a038fc2"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimFenceDocument](riverhog-protocol-processingclaimfencedocument.md)

## Governing policies

- <a id="pa-246d665ced"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimFenceDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0074a72334e274fe20a45dce25c55a8205dd795d1e04ffdb3ff1bf2d81be55e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimFenceDocument",
  "unit": "member"
}
```

</details>
