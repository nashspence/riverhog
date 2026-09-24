# riverhog_client.processing.ClaimedRetrieval.renew

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval-renew:691f3d5e32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-73b2ba2f56"></a>
- <a id="s-34736baa51"></a>`distribution`: `riverhog-client`
- <a id="s-5d08cc8f43"></a>`module`: `riverhog_client.processing`
- <a id="s-689b7dcffa"></a>`name`: `renew`
- <a id="s-7bdc10b070"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-1ec580bbdd"></a>`unit`: `member`

### Declared structure

- <a id="s-4354088648"></a>`kind`: `"method"`
- <a id="s-5f0cd7df25"></a>`signature`: `"\"(self, *, lease_seconds: 'int') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-3963197d68"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.renew`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6adf429578e89735e9c030439cb4b26eaa0738ee800124e7d3b8c4a5f783d06 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, lease_seconds: 'int') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "renew",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
