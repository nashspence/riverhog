# stove0_core.DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-default-operational-state-ret-4306b3b54e:d620687d62 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c3cc6ea186"></a>
- <a id="s-713db2ae41"></a>`distribution`: `stove0-server`
- <a id="s-a779d65c1b"></a>`module`: `stove0_core`
- <a id="s-16a55b885a"></a>`name`: `DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS`
- <a id="s-e18b2be974"></a>`unit`: `export`

### Declared structure

- <a id="s-5a2776fff0"></a>`kind`: `"constant"`
- <a id="s-ee621f49f7"></a>`value`: `2592000`

## Governing policies

- <a id="pa-5e232ca8e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33ee0837f68768a70d9f595e63121d649c44ea1cdeae03c578b0aa2377c9f734 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 2592000
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS",
  "unit": "export"
}
```

</details>
