# riverhog_client.processing.ClaimedRetrieval.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval-close:c2f7b68a20 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-778d0bfcce"></a>
- <a id="s-03c7365f91"></a>`distribution`: `riverhog-client`
- <a id="s-bfdda1f2c5"></a>`module`: `riverhog_client.processing`
- <a id="s-d8addb699d"></a>`name`: `close`
- <a id="s-67eb9414a6"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-d963e0f49e"></a>`unit`: `member`

### Declared structure

- <a id="s-66603fad82"></a>`kind`: `"method"`
- <a id="s-02615b754b"></a>`signature`: `"\"(self, *, success: 'bool' = True) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-9a8226d585"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5298b46754368bf983f8604294fcd9a98e03f106358d1eafae29b7f418bcf200 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, success: 'bool' = True) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "close",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
