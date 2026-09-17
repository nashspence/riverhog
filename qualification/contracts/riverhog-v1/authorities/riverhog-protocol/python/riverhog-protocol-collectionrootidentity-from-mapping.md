# riverhog_protocol.CollectionRootIdentity.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionrootidentity-fa7fbef038:dde3fa4c05 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ab3bb96a2"></a>
- <a id="s-c1856531e2"></a>`distribution`: `riverhog-protocol`
- <a id="s-a6b0939308"></a>`module`: `riverhog_protocol`
- <a id="s-cd85ab459d"></a>`name`: `from_mapping`
- <a id="s-609a3eb728"></a>`owner`: `riverhog_protocol.CollectionRootIdentity`
- <a id="s-3f1a787914"></a>`unit`: `member`

### Declared structure

- <a id="s-7ff5a3e43c"></a>`kind`: `"classmethod"`
- <a id="s-bc05167ed8"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'CollectionRootIdentity'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootIdentity](riverhog-protocol-collectionrootidentity.md)

## Governing policies

- <a id="pa-e38510fe49"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionRootIdentity.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 291e2405ac1e0ef67740cf5d493dd25ca8048fadc927de7d661ff45c74b7c517 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'CollectionRootIdentity'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.CollectionRootIdentity",
  "unit": "member"
}
```

</details>
