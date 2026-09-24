# riverhog_client.processing.ClaimedRetrieval.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval-enter:bf9005cd2a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dec550d51a"></a>
- <a id="s-0aa9379e0a"></a>`distribution`: `riverhog-client`
- <a id="s-410f0a81c4"></a>`module`: `riverhog_client.processing`
- <a id="s-5eb0839829"></a>`name`: `__enter__`
- <a id="s-121161e949"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-2ca8bc2507"></a>`unit`: `member`

### Declared structure

- <a id="s-b063c5ccf9"></a>`kind`: `"method"`
- <a id="s-621f894ca7"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-eed96da932"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d87458b156cddbddd5173e78d3b7046536df517a7becd7a5debb793aaea92fcb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__enter__",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
