# a_riverhog_s3_store_lib.create_s3_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-create-s3-client:0199c640e2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f7cd2392a"></a>
- <a id="s-696ca09eb5"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-607cb27fd9"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-8a55912a65"></a>`name`: `create_s3_client`
- <a id="s-f746498ace"></a>`unit`: `export`

### Declared structure

- <a id="s-52428680ca"></a>`kind`: `"function"`
- <a id="s-303c8c515a"></a>`signature`: `"\"(config: 'S3ClientConfig', *, tuning: 'S3TransportTuning \| None' = None) -> 'Any'\""`

## Governing policies

- <a id="pa-efc0b05e80"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.create_s3_client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d5a5d8347b34f2fe0531d18791c8bd677f5066ee29565212adbae7fc5a59e37d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config: 'S3ClientConfig', *, tuning: 'S3TransportTuning | None' = None) -> 'Any'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "create_s3_client",
  "unit": "export"
}
```

</details>
