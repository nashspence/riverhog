# riverhog_client.processing.ClaimedRetrieval.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval-exit:1a62006bea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0e8d1021a"></a>
- <a id="s-9c777bca37"></a>`distribution`: `riverhog-client`
- <a id="s-7924e57e5a"></a>`module`: `riverhog_client.processing`
- <a id="s-c668a2c3ef"></a>`name`: `__exit__`
- <a id="s-285e04ceb7"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-71f6292c10"></a>`unit`: `member`

### Declared structure

- <a id="s-337069f540"></a>`kind`: `"method"`
- <a id="s-537ba866ed"></a>`signature`: `"\"(self, exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-c505c917fa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c726441311c36d44a8fb78ff94c10e12481bde195284c58b19bd55469cca98de -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__exit__",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
