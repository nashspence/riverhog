# riverhog_recover.RecoveredCollectionTags.iter_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recoveredcollectiontags-iter-tags:3862fc0dfd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06976c1df8"></a>
- <a id="s-e36141a659"></a>`distribution`: `riverhog-recover`
- <a id="s-42efb21eb2"></a>`module`: `riverhog_recover`
- <a id="s-431075db04"></a>`name`: `iter_tags`
- <a id="s-13ff07f5de"></a>`owner`: `riverhog_recover.RecoveredCollectionTags`
- <a id="s-e589717d82"></a>`unit`: `member`

### Declared structure

- <a id="s-f3cb840af2"></a>`kind`: `"method"`
- <a id="s-1504a06eac"></a>`signature`: `"\"(self) -> 'Iterator[str]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_recover.RecoveredCollectionTags](riverhog-recover-recoveredcollectiontags.md)

## Governing policies

- <a id="pa-a2e28f4ea9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources.md#src-dbfe6c5e2e) — `reference/riverhog/recovery/src/riverhog_recover/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_recover.RecoveredCollectionTags.iter_tags`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 108f6539ad43a14d7ba028d744836b9005f430e577b696c592ca0638668cfc5a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Iterator[str]'\""
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "iter_tags",
  "owner": "riverhog_recover.RecoveredCollectionTags",
  "unit": "member"
}
```
