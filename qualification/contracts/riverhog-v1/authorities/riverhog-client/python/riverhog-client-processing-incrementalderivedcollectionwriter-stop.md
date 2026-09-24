# riverhog_client.processing.IncrementalDerivedCollectionWriter.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-incrementalder-663944a4f5:6703705755 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8771e96e4c"></a>
- <a id="s-ae60bd420f"></a>`distribution`: `riverhog-client`
- <a id="s-600a06bb91"></a>`module`: `riverhog_client.processing`
- <a id="s-2f4bfd804f"></a>`name`: `stop`
- <a id="s-ad17d54109"></a>`owner`: `riverhog_client.processing.IncrementalDerivedCollectionWriter`
- <a id="s-2b38f358fb"></a>`unit`: `member`

### Declared structure

- <a id="s-49fd98d775"></a>`kind`: `"method"`
- <a id="s-90f87edcb7"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [IncrementalDerivedCollectionWriter](riverhog-client-processing-incrementalderivedcollectionwriter.md)

## Governing policies

- <a id="pa-64e5e94aea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.IncrementalDerivedCollectionWriter.stop`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8c42a771eb3e5f19ede16f027099781a54c77b9b4818c566a54976e5e41fab0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "stop",
  "owner": "riverhog_client.processing.IncrementalDerivedCollectionWriter",
  "unit": "member"
}
```

</details>
