# riverhog_client.transform.ClaimedRetrieval.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval-exit:ae26e71f77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-40c8a142e2"></a>
- <a id="s-369fe98b91"></a>`distribution`: `riverhog-client`
- <a id="s-b2df4e146e"></a>`module`: `riverhog_client.transform`
- <a id="s-c66119122c"></a>`name`: `__exit__`
- <a id="s-1bf3612dac"></a>`owner`: `riverhog_client.transform.ClaimedRetrieval`
- <a id="s-4e78c2aad9"></a>`unit`: `member`

### Declared structure

- <a id="s-96658453a1"></a>`kind`: `"method"`
- <a id="s-2147718859"></a>`signature`: `"\"(self, exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-0b889243c7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.__exit__`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f557c14343e0b1c80e52ca1ee7835d5a76743f13e1f372696154a1e8644701ce -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "__exit__",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```
