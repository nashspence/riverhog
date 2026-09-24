# riverhog_client.processing.ClaimedRetrieval.closed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval-closed:953b823bff -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1109433482"></a>
- <a id="s-0f8bc40cc4"></a>`distribution`: `riverhog-client`
- <a id="s-df25ecfa90"></a>`module`: `riverhog_client.processing`
- <a id="s-84ac0252d5"></a>`name`: `closed`
- <a id="s-e21da25c52"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-e7fb2b9c0a"></a>`unit`: `member`

### Declared structure

- <a id="s-ac59ecc78c"></a>`kind`: `"property"`
- <a id="s-5d769e4c41"></a>`signature`: `"\"(self) -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-2f10b85875"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.closed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8379124da1fc71b2b6d05bee18c2eee82e6ab3db69f85cb19e3f21ecb25a2a9 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "closed",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
