# riverhog_protocol.ProcessingClaimAbandonDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimabandondocument-get:214d0ff2df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-04baf18c85"></a>
- <a id="s-c13862cc7e"></a>`distribution`: `riverhog-protocol`
- <a id="s-7245ebb2bd"></a>`module`: `riverhog_protocol`
- <a id="s-30608aa09e"></a>`name`: `get`
- <a id="s-03aa4e2440"></a>`owner`: `riverhog_protocol.ProcessingClaimAbandonDocument`
- <a id="s-92bcd65b8b"></a>`unit`: `member`

### Declared structure

- <a id="s-1a30af6719"></a>`kind`: `"method"`
- <a id="s-6fd75d6539"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimAbandonDocument](riverhog-protocol-processingclaimabandondocument.md)

## Governing policies

- <a id="pa-05549248be"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimAbandonDocument.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dce2631f55291d9f15b90e9c19a440bb9c3f6268956a43d1ebed67231a648aa5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimAbandonDocument",
  "unit": "member"
}
```
