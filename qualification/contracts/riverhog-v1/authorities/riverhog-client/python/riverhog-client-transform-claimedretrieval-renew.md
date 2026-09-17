# riverhog_client.transform.ClaimedRetrieval.renew

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval-renew:5636d98721 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63159fd7a7"></a>
- <a id="s-a09268caa2"></a>`distribution`: `riverhog-client`
- <a id="s-e1ff57d7ae"></a>`module`: `riverhog_client.transform`
- <a id="s-9997854a7e"></a>`name`: `renew`
- <a id="s-dd41721135"></a>`owner`: `riverhog_client.transform.ClaimedRetrieval`
- <a id="s-66b7684dee"></a>`unit`: `member`

### Declared structure

- <a id="s-22edb8ae7e"></a>`kind`: `"method"`
- <a id="s-011c680819"></a>`signature`: `"\"(self, *, lease_seconds: 'int') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-268c05c3ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.renew`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09ca466e46ff16c7eead33ce1d10a8d2fcd2531c2442a44a068e01dffa70aca3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, lease_seconds: 'int') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "renew",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
