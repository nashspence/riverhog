# riverhog_client.transform.ClaimedArtifact.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedartifact-as-dict:b572ae6306 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0cfb8af67a"></a>
- <a id="s-eefe440c4a"></a>`distribution`: `riverhog-client`
- <a id="s-a258ac1bf1"></a>`module`: `riverhog_client.transform`
- <a id="s-28bc2f2c22"></a>`name`: `as_dict`
- <a id="s-293714df48"></a>`owner`: `riverhog_client.transform.ClaimedArtifact`
- <a id="s-4ba55a7e07"></a>`unit`: `member`

### Declared structure

- <a id="s-265369d04b"></a>`kind`: `"method"`
- <a id="s-d3fcab743e"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedArtifact](riverhog-client-transform-claimedartifact.md)

## Governing policies

- <a id="pa-61aa3f67cb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedArtifact.as_dict`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 554c26f9a380592e231fa128716a11b40b885ae7a32c91e35d299a381d60e24f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "as_dict",
  "owner": "riverhog_client.transform.ClaimedArtifact",
  "unit": "member"
}
```

</details>
