# stove0_observer_protocol.SemanticValidatorRegistry.resolve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidato-56bc3b40a1:a56acc1eb9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fdb429a35c"></a>
- <a id="s-c68bc8ece4"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-94ba0ec1df"></a>`module`: `stove0_observer_protocol`
- <a id="s-e04762cbbf"></a>`name`: `resolve`
- <a id="s-7427e0e2ad"></a>`owner`: `stove0_observer_protocol.SemanticValidatorRegistry`
- <a id="s-667bc7d5ba"></a>`unit`: `member`

### Declared structure

- <a id="s-7a37883491"></a>`kind`: `"method"`
- <a id="s-8d37cf7b46"></a>`signature`: `"\"(self, profile_id: 'str', profile_sha256: 'str') -> 'FactsSemanticValidator \| None'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidatorRegistry](stove0-observer-protocol-semanticvalidatorregistry.md)

## Governing policies

- <a id="pa-bf202ebfb2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidatorRegistry.resolve`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f633a3be666f8d508c30d8e4ef7453dd9731e538c26c1ea11f5863df1290600 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, profile_id: 'str', profile_sha256: 'str') -> 'FactsSemanticValidator | None'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "resolve",
  "owner": "stove0_observer_protocol.SemanticValidatorRegistry",
  "unit": "member"
}
```

</details>
