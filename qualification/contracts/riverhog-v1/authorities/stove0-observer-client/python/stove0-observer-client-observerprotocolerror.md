# stove0_observer_client.ObserverProtocolError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client-observerprotocolerror:2a0a9f79ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-60cb40e27e"></a>
- <a id="s-ff70f223ea"></a>`distribution`: `stove0-observer-client`
- <a id="s-c7ac5d94c9"></a>`module`: `stove0_observer_client`
- <a id="s-f1edd5a1df"></a>`name`: `ObserverProtocolError`
- <a id="s-668bafea39"></a>`unit`: `export`

### Declared structure

- <a id="s-4133586983"></a>`kind`: `"class"`
- <a id="s-c14555570f"></a>`signature`: `"'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\', \\'unsupported_semantics\\']\", code: \\'str \| None\\' = None, observed_status: \\'int \| None\\' = None, details: \\'Mapping[str, Any] \| None\\' = None) -> \\'None\\''"`

## Governing policies

- <a id="pa-bad38b6ce1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-client:stove0_observer_client](../../../evidence/sources.md#src-67dbe161ba) — [reference/stove0/packages/observer-client/src/stove0\_observer\_client/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_client.ObserverProtocolError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28b351785dcd3df25a69f2760c302cfad12ef9eab94bc1c9a858b301dbc8e1be -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\', \\'unsupported_semantics\\']\", code: \\'str | None\\' = None, observed_status: \\'int | None\\' = None, details: \\'Mapping[str, Any] | None\\' = None) -> \\'None\\''"
  },
  "distribution": "stove0-observer-client",
  "module": "stove0_observer_client",
  "name": "ObserverProtocolError",
  "unit": "export"
}
```

</details>
