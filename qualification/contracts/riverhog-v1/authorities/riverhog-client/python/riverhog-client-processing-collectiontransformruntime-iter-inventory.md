# riverhog_client.processing.CollectionTransformRuntime.iter_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-b72b46afbc:084ce01e43 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-51621713ac"></a>
- <a id="s-a243696cc6"></a>`distribution`: `riverhog-client`
- <a id="s-38f8672908"></a>`module`: `riverhog_client.processing`
- <a id="s-c25c7b5755"></a>`name`: `iter_inventory`
- <a id="s-45bc8a2a94"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-8e612e06b7"></a>`unit`: `member`

### Declared structure

- <a id="s-5e6ef71df8"></a>`kind`: `"method"`
- <a id="s-491adaa222"></a>`signature`: `"'(self)'"`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-bc00176cb2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.iter_inventory`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b239eac91807589c96eea2cd56d4dd49ec69eb9a855d36c9a9be73fceef9feba -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "'(self)'"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "iter_inventory",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
