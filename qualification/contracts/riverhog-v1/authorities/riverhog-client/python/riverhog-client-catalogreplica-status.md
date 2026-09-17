# riverhog_client.CatalogReplica.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica-status:34ab2faa87 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-59c19d5948"></a>
- <a id="s-b3edc06291"></a>`distribution`: `riverhog-client`
- <a id="s-f70ab69f4f"></a>`module`: `riverhog_client`
- <a id="s-8e43ec15c8"></a>`name`: `status`
- <a id="s-ad500e921e"></a>`owner`: `riverhog_client.CatalogReplica`
- <a id="s-b3f9c5b9eb"></a>`unit`: `member`

### Declared structure

- <a id="s-fb25802e75"></a>`kind`: `"method"`
- <a id="s-40255bfe0f"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [CatalogReplica](riverhog-client-catalogreplica.md)

## Governing policies

- <a id="pa-ced5ead630"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 996aa0a6e7a68c1c7e738c3d9555018e5399a3ce847fa4d1ce93494390c37005 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "status",
  "owner": "riverhog_client.CatalogReplica",
  "unit": "member"
}
```

</details>
