# riverhog_protocol.ProcessingClaimCreateDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimcreatedocument-get:81eb36e0c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c8b6ecd00"></a>
- <a id="s-b522157303"></a>`distribution`: `riverhog-protocol`
- <a id="s-018a4d00c6"></a>`module`: `riverhog_protocol`
- <a id="s-00582100bd"></a>`name`: `get`
- <a id="s-5e5f6478ab"></a>`owner`: `riverhog_protocol.ProcessingClaimCreateDocument`
- <a id="s-1cf1790f90"></a>`unit`: `member`

### Declared structure

- <a id="s-da7a48995d"></a>`kind`: `"method"`
- <a id="s-f1adc842a4"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimCreateDocument](riverhog-protocol-processingclaimcreatedocument.md)

## Governing policies

- <a id="pa-94bd78bad5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimCreateDocument.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e83d207343401451ef0c75ddeccc0185a69c19342bcf1fad9ad3688fffb051e5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimCreateDocument",
  "unit": "member"
}
```
