# generated:stove0-observer protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol:stove0-observer-support:generated-stove0-observer-protocol:dc23edfcb6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Process Protocol](index.md) |

## External contract

<a id="s-1eafdfd27d"></a>
<a id="s-2413957c85"></a>
<a id="s-c7c5590254"></a>

| Field | Value |
|---|---|
| <a id="s-91230d9f6f"></a>`authorities · http_operations` | `"http_binding.operations"` |
| <a id="s-97fdac27e7"></a>`authorities · semantic_acceptance` | `"semantic_acceptance"` |
| <a id="s-0c79c0ec56"></a>`authorities · structural_models` | `"schemas"` |
| <a id="s-d5ea9dbeda"></a>`bundle_sha256` | `"dffbe64ecc9436f98dbe3aca0c774ca4cee9d39b54469fdc288caf3839484aef"` |
| <a id="s-30491cc055"></a>`compatibility · contract_identity` | `"canonical-json-sha256"` |
| <a id="s-8476a6bdf5"></a>`compatibility · unknown_fields` | `"reject"` |
| <a id="s-2f76b229b5"></a>`compatibility · unknown_protocol_revision` | `"reject"` |
| <a id="s-7ec445dc33"></a>`format` | `"stove0-observer-schema-bundle/v1"` |
| <a id="s-afb71a9619"></a>`protocol` | `"stove0-content-observer/v1"` |
| <a id="s-bcaa1f996a"></a>`semantic_acceptance · binding` | `"ObserverContract.facts_semantics"` |
| <a id="s-29059e1307"></a>`semantic_acceptance · identity` | `["id","profile_sha256"]` |
| <a id="s-fbcc964da3"></a>`semantic_acceptance · kind` | `"profile-registry"` |
| <a id="s-c8966464fd"></a>`semantic_acceptance · unavailable_profile` | `"reject"` |

## Maintained corroboration

### Related interface records

- [GET /v1/observer](../process-protocol-operations/get-v1-observer.md)
- [POST /v1/observe](../process-protocol-operations/post-v1-observe.md)
- [generated:stove0-observer: ContentObservationInvocation](../process-protocol-schemas/generated-stove0-observer-contentobservationinvocation.md)
- [generated:stove0-observer: ContentObservationRequest](../process-protocol-schemas/generated-stove0-observer-contentobservationrequest.md)
- [generated:stove0-observer: ContentObservationResult](../process-protocol-schemas/generated-stove0-observer-contentobservationresult.md)
- [generated:stove0-observer: ErrorResponse](../process-protocol-schemas/generated-stove0-observer-errorresponse.md)
- [generated:stove0-observer: ObserverConformanceResult](../process-protocol-schemas/generated-stove0-observer-observerconformanceresult.md)
- [generated:stove0-observer: ObserverContract](../process-protocol-schemas/generated-stove0-observer-observercontract.md)
- [generated:stove0-observer: ObserverDescriptor](../process-protocol-schemas/generated-stove0-observer-observerdescriptor.md)

## Governing policies

- <a id="pa-bf2ab137ad"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-observer](../../../evidence/sources/authorities.md#src-dcc0b5485b) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/schemas.py::observer\_schema\_bundle](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/authorities`
- `/external_contract/protocol_schemas/generated:stove0-observer/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-observer/compatibility`
- `/external_contract/protocol_schemas/generated:stove0-observer/format`
- `/external_contract/protocol_schemas/generated:stove0-observer/protocol`
- `/external_contract/protocol_schemas/generated:stove0-observer/semantic_acceptance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:stove0-observer/authorities`

<!-- exact-contract-value: 723bdc629c692bd71cc54f046e71fc791297a8c6a984b85d1f0962ee4c72194e -->

```json
{
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:stove0-observer/bundle_sha256`

<!-- exact-contract-value: 0e6fe00956a7e13da550379e9a574ceb6b5d0658b79f3db3cfb04fc515551238 -->

```json
"dffbe64ecc9436f98dbe3aca0c774ca4cee9d39b54469fdc288caf3839484aef"
```

### `/external_contract/protocol_schemas/generated:stove0-observer/compatibility`

<!-- exact-contract-value: 2cbdd4f0020ba7220dc9f1a5f82d0e1149904af6dad73a4daedfc2bd6cb3f2c3 -->

```json
{
  "contract_identity": "canonical-json-sha256",
  "unknown_fields": "reject",
  "unknown_protocol_revision": "reject"
}
```

### `/external_contract/protocol_schemas/generated:stove0-observer/format`

<!-- exact-contract-value: 065211257aaa3d23713fbecd42883a406ec11bb49c25702110eabd34a3ca9367 -->

```json
"stove0-observer-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:stove0-observer/protocol`

<!-- exact-contract-value: 323e380a7c090e7fc1fcabf5c1f93281781a1762ee6703d2888b13179f9083cd -->

```json
"stove0-content-observer/v1"
```

### `/external_contract/protocol_schemas/generated:stove0-observer/semantic_acceptance`

<!-- exact-contract-value: 4c9c2fc956c554081c04e778a6984703fcf498e5e081c1da627e71d810898308 -->

```json
{
  "binding": "ObserverContract.facts_semantics",
  "identity": [
    "id",
    "profile_sha256"
  ],
  "kind": "profile-registry",
  "unavailable_profile": "reject"
}
```

</details>
