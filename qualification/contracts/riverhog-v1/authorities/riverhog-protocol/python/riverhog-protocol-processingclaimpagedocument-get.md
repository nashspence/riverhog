# riverhog_protocol.ProcessingClaimPageDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimpagedocument-get:f2793d2ec8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c309b438c"></a>
- <a id="s-bce635ab20"></a>`distribution`: `riverhog-protocol`
- <a id="s-e745f02693"></a>`module`: `riverhog_protocol`
- <a id="s-28a17f9b2f"></a>`name`: `get`
- <a id="s-bf630e2d54"></a>`owner`: `riverhog_protocol.ProcessingClaimPageDocument`
- <a id="s-e050a7f6c0"></a>`unit`: `member`

### Declared structure

- <a id="s-cde99ef0cd"></a>`kind`: `"method"`
- <a id="s-117e77e9c1"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimPageDocument](riverhog-protocol-processingclaimpagedocument.md)

## Governing policies

- <a id="pa-a4dc059f34"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPageDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8c52412438a73eaf6067c1e9ea4e6a30133edacdb02afa04054cb1d772b091a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimPageDocument",
  "unit": "member"
}
```

</details>
