# riverhog_client.processing.DerivedCollectionWriter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-derivedcollectionwriter:c1abb0c910 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-332238ec89"></a>
- <a id="s-11eaff2c7d"></a>`distribution`: `riverhog-client`
- <a id="s-fae3b73f71"></a>`module`: `riverhog_client.processing`
- <a id="s-9bb3e2c3b5"></a>`name`: `DerivedCollectionWriter`
- <a id="s-3d102d0d2e"></a>`unit`: `export`

### Declared structure

- <a id="s-f226828545"></a>`kind`: `"class"`
- <a id="s-8fcabfaa66"></a>`signature`: `"\"(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str' = 'development') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [replace_api](riverhog-client-processing-derivedcollectionwriter-replace-api.md)
- [publish](riverhog-client-processing-derivedcollectionwriter-publish.md)

## Governing policies

- <a id="pa-b4e5b3726c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.DerivedCollectionWriter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8146ff161da7ab69633a0fbc903263836797d32d6fc74d56a6fa5e74fdcfd60f -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str' = 'development') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "DerivedCollectionWriter",
  "unit": "export"
}
```

</details>
