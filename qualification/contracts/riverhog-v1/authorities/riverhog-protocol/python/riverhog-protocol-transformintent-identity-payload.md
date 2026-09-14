# riverhog_protocol.TransformIntent.identity_payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformintent-identity-payload:76e00a1c28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5079f9541a"></a>
- <a id="s-2aef18ddda"></a>`distribution`: `riverhog-protocol`
- <a id="s-054d96e67e"></a>`module`: `riverhog_protocol`
- <a id="s-52424fe8b2"></a>`name`: `identity_payload`
- <a id="s-4b5aa04fd0"></a>`owner`: `riverhog_protocol.TransformIntent`
- <a id="s-2f3f9179ff"></a>`unit`: `member`

### Declared structure

- <a id="s-6e202095da"></a>`kind`: `"method"`
- <a id="s-d54c908181"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformIntent](riverhog-protocol-transformintent.md)

## Governing policies

- <a id="pa-47f21a5824"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformIntent.identity_payload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14f7a7273ba07ee7476633da8facd7674ca92c281ae700f738a2894fee873676 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "identity_payload",
  "owner": "riverhog_protocol.TransformIntent",
  "unit": "member"
}
```
