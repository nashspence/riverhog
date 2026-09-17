# stove0_core.TargetPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport:ef9508ec89 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9ac746a3c"></a>
- <a id="s-75f089bdb4"></a>`distribution`: `stove0-server`
- <a id="s-4d8a896dc0"></a>`module`: `stove0_core`
- <a id="s-e18209cee7"></a>`name`: `TargetPort`
- <a id="s-10f16b65b2"></a>`unit`: `export`

### Declared structure

- <a id="s-7d373c9f56"></a>`kind`: `"class"`
- <a id="s-59f29640eb"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [cancel_job](stove0-core-targetport-cancel-job.md)
- [contract](stove0-core-targetport-contract.md)
- [get_job](stove0-core-targetport-get-job.md)
- [preflight](stove0-core-targetport-preflight.md)
- [put_job](stove0-core-targetport-put-job.md)

## Governing policies

- <a id="pa-8ef51f75df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetPort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 672f8f7ce3e2339c3d82ef624acd594479f8687e2458715f465a293384160ab3 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "TargetPort",
  "unit": "export"
}
```

</details>
