# riverhog_client.transform.ClaimedRetrieval.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval-close:fcada33436 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34f11d3148"></a>
- <a id="s-d686193dc4"></a>`distribution`: `riverhog-client`
- <a id="s-9c8176ef65"></a>`module`: `riverhog_client.transform`
- <a id="s-4c62fb2310"></a>`name`: `close`
- <a id="s-7d3aa75d6d"></a>`owner`: `riverhog_client.transform.ClaimedRetrieval`
- <a id="s-dd97639916"></a>`unit`: `member`

### Declared structure

- <a id="s-1d8de0a6a6"></a>`kind`: `"method"`
- <a id="s-8ab3e3dbf5"></a>`signature`: `"\"(self, *, success: 'bool' = True) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-936f29b898"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bacea339f3d8de4c06947eabc5c6673341f38c84d298316c867b4fea12e51aeb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, success: 'bool' = True) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "close",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
