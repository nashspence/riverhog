# riverhog_client.transform.CollectionTransformRuntime.heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-c1ebc56391:cee1fdd5b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2797735885"></a>
- <a id="s-6c73690486"></a>`distribution`: `riverhog-client`
- <a id="s-03352a6703"></a>`module`: `riverhog_client.transform`
- <a id="s-ef9b6552b3"></a>`name`: `heartbeat`
- <a id="s-6b90b01a2c"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-64443c2f2f"></a>`unit`: `member`

### Declared structure

- <a id="s-0b4003f630"></a>`kind`: `"method"`
- <a id="s-a9554441fc"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-d482a5a9ae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.heartbeat`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f2e481e1f4f63b658ef462ac7fc548032a430525cc58304820ee9febe551076 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "heartbeat",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
