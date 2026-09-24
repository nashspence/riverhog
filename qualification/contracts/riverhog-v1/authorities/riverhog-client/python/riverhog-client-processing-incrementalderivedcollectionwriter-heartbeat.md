# riverhog_client.processing.IncrementalDerivedCollectionWriter.heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-incrementalder-439f47ba24:52cb5b4496 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2251847ce"></a>
- <a id="s-07356843f9"></a>`distribution`: `riverhog-client`
- <a id="s-ad5073261f"></a>`module`: `riverhog_client.processing`
- <a id="s-6287d96df8"></a>`name`: `heartbeat`
- <a id="s-a3398dd68a"></a>`owner`: `riverhog_client.processing.IncrementalDerivedCollectionWriter`
- <a id="s-b197b646a2"></a>`unit`: `member`

### Declared structure

- <a id="s-e743fd832b"></a>`kind`: `"method"`
- <a id="s-2b70474483"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [IncrementalDerivedCollectionWriter](riverhog-client-processing-incrementalderivedcollectionwriter.md)

## Governing policies

- <a id="pa-86dcf9625e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.IncrementalDerivedCollectionWriter.heartbeat`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7775f3fec67946247bab7f12c8f048de38223e20aab282deb276defc02a38b0c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "heartbeat",
  "owner": "riverhog_client.processing.IncrementalDerivedCollectionWriter",
  "unit": "member"
}
```

</details>
