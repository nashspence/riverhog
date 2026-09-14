# riverhog_protocol.TransformIntent.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformintent-to-json-bytes:eb9069fed9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-248efcd5b8"></a>
- <a id="s-f17bd23cf6"></a>`distribution`: `riverhog-protocol`
- <a id="s-9d650f3600"></a>`module`: `riverhog_protocol`
- <a id="s-88b8a34143"></a>`name`: `to_json_bytes`
- <a id="s-f806514a73"></a>`owner`: `riverhog_protocol.TransformIntent`
- <a id="s-e9854be54d"></a>`unit`: `member`

### Declared structure

- <a id="s-c7b9fec161"></a>`kind`: `"method"`
- <a id="s-2863975ea9"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformIntent](riverhog-protocol-transformintent.md)

## Governing policies

- <a id="pa-7a965ef84b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformIntent.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22dd75c8f2ec8dfeddb3dadf37a9d83220fc352c20a323a3b62ccd72163452b7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "to_json_bytes",
  "owner": "riverhog_protocol.TransformIntent",
  "unit": "member"
}
```
