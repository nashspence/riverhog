# stove0_target_protocol.TargetJobRequest.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobrequest-v-4af02de8d5:5c758d720b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8cf099437f"></a>
- <a id="s-b6b67d9dd9"></a>`distribution`: `stove0-target-protocol`
- <a id="s-19e36ad35a"></a>`module`: `stove0_target_protocol`
- <a id="s-73c3ccc89e"></a>`name`: `verify_digest`
- <a id="s-3450c5c583"></a>`owner`: `stove0_target_protocol.TargetJobRequest`
- <a id="s-c9d1bd667d"></a>`unit`: `member`

### Declared structure

- <a id="s-477e6270b5"></a>`kind`: `"method"`
- <a id="s-83a1ee85db"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetJobRequest](stove0-target-protocol-targetjobrequest.md)

## Governing policies

- <a id="pa-eea33f1670"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobRequest.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee31734afc861fca44ce56528ce556639dfd40ff30484dcff2e193850dc14ea7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.TargetJobRequest",
  "unit": "member"
}
```
