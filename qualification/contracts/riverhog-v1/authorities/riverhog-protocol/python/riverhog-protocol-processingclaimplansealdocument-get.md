# riverhog_protocol.ProcessingClaimPlanSealDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimplanseal-f36eac9e5a:cc6faec302 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cfc72571ae"></a>
- <a id="s-363d3b058b"></a>`distribution`: `riverhog-protocol`
- <a id="s-1c0a7e5aa1"></a>`module`: `riverhog_protocol`
- <a id="s-dda12cc443"></a>`name`: `get`
- <a id="s-ccd1200b1f"></a>`owner`: `riverhog_protocol.ProcessingClaimPlanSealDocument`
- <a id="s-019e095658"></a>`unit`: `member`

### Declared structure

- <a id="s-83849a6876"></a>`kind`: `"method"`
- <a id="s-8e58531e13"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimPlanSealDocument](riverhog-protocol-processingclaimplansealdocument.md)

## Governing policies

- <a id="pa-96bd4d81ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPlanSealDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 520915e2c4c07e79b33fd2e2f2b1e8052915b5da02e817c7fdf399183e4a4666 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimPlanSealDocument",
  "unit": "member"
}
```

</details>
