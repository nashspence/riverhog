# generated:stove0-observer protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-protocol:f27802529a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `protocol` |
| Family | `protocol` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: items=additional keys=`http_operations`, `semantic_acceptance`, `structural_models` | "d7ee01cb66cac26b457031d54882a95ce2b1ae0e163df53590fee00ae1a8f0ce" | additional keys=`contract_identity`, `unknown_fields`, `unknown_protocol_revision` | "stove0-observer-schema-bundle/v1" | additional keys=`operations` | "stove0-content-observer/v1" | additional keys=`binding`, `identity`, `kind`, `unavailable_profile`

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-observer` — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/authorities`
- `/external_contract/protocol_schemas/generated:stove0-observer/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-observer/compatibility`
- `/external_contract/protocol_schemas/generated:stove0-observer/format`
- `/external_contract/protocol_schemas/generated:stove0-observer/http_binding`
- `/external_contract/protocol_schemas/generated:stove0-observer/protocol`
- `/external_contract/protocol_schemas/generated:stove0-observer/semantic_acceptance`

### Exact owned JSON

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

<!-- exact-contract-value: 51bba52db678f3e30f171c5bf012f1fb0c17a6fb3dd1cdae46441fe29a50fa94 -->

```json
"d7ee01cb66cac26b457031d54882a95ce2b1ae0e163df53590fee00ae1a8f0ce"
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

### `/external_contract/protocol_schemas/generated:stove0-observer/http_binding`

<!-- exact-contract-value: b7be4a50582b9ec61353571eb06fb8ff5413e9b413d8ff75bf348c6444648c05 -->

```json
{
  "operations": [
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "bad_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "observer_failed",
          "status": 500
        }
      ],
      "method": "GET",
      "path": "/v1/observer",
      "path_parameters": [],
      "request": {
        "kind": "none",
        "schema": null
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "ObserverDescriptor",
        "statuses": [
          200
        ]
      }
    },
    {
      "error_schema": "ErrorResponse",
      "errors": [
        {
          "code": "invalid_observation_request",
          "status": 400
        },
        {
          "code": "unauthorized",
          "status": 401
        },
        {
          "code": "request_too_large",
          "status": 413
        },
        {
          "code": "observer_failed",
          "status": 500
        }
      ],
      "method": "POST",
      "path": "/v1/observe",
      "path_parameters": [],
      "request": {
        "kind": "json",
        "schema": "ObservationInvocation"
      },
      "response": {
        "headers": [],
        "kind": "json",
        "schema": "ObservationResult",
        "statuses": [
          200
        ]
      }
    }
  ]
}
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
