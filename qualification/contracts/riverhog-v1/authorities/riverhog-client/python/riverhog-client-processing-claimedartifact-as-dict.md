# riverhog_client.processing.ClaimedArtifact.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedartifact-as-dict:0533ddaa39 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8afdd01725"></a>
- <a id="s-e9f8ef532f"></a>`distribution`: `riverhog-client`
- <a id="s-e05069caab"></a>`module`: `riverhog_client.processing`
- <a id="s-ba55e69f0d"></a>`name`: `as_dict`
- <a id="s-328f634d1c"></a>`owner`: `riverhog_client.processing.ClaimedArtifact`
- <a id="s-0e9fa321e5"></a>`unit`: `member`

### Declared structure

- <a id="s-0b5abbfc72"></a>`kind`: `"method"`
- <a id="s-b0b817cd99"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedArtifact](riverhog-client-processing-claimedartifact.md)

## Governing policies

- <a id="pa-488467cc5a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedArtifact.as_dict`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2103010894b4e5417426451cbe233cc7d0172a82cb77b2b1c1672b4be456943e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "as_dict",
  "owner": "riverhog_client.processing.ClaimedArtifact",
  "unit": "member"
}
```

</details>
