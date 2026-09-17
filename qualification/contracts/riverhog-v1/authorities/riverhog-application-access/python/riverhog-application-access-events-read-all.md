# riverhog_application_access.EVENTS_READ_ALL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-events-read-all:2f6d60c668 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd2dac9fe8"></a>
- <a id="s-e3b9c6a50f"></a>`distribution`: `riverhog-application-access`
- <a id="s-e92ebb643b"></a>`module`: `riverhog_application_access`
- <a id="s-edbda55c68"></a>`name`: `EVENTS_READ_ALL`
- <a id="s-7a0b4fd81e"></a>`unit`: `export`

### Declared structure

- <a id="s-572ec24809"></a>`kind`: `"constant"`
- <a id="s-82d112a0b0"></a>`value`: `"events:read_all"`

## Governing policies

- <a id="pa-62dd7d8865"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.EVENTS_READ_ALL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00429d2e0fbc90cb31867c09f4b6072315e5b2a9e24ec629b4d130a1d20bf801 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "events:read_all"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "EVENTS_READ_ALL",
  "unit": "export"
}
```

</details>
