# generated:stove0-target protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol:stove0-target-support:generated-stove0-target-protocol:133ae2e83a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Process Protocol](index.md) |

## External contract

<a id="s-19ff2c9676"></a>
<a id="s-4ebc473534"></a>
<a id="s-9269284132"></a>
<a id="s-459baccad4"></a>

| Field | Value |
|---|---|
| <a id="s-e4684f2b36"></a>`authorities · departure_http_operations` | `"departure_effect.http_binding.operations"` |
| <a id="s-28fe93e0c1"></a>`authorities · departure_structural_models` | `"departure_effect.schemas"` |
| <a id="s-5cae7e4762"></a>`authorities · http_operations` | `"http_binding.operations"` |
| <a id="s-33a0a01689"></a>`authorities · semantic_acceptance` | `"semantic_acceptance"` |
| <a id="s-6508281ade"></a>`authorities · structural_models` | `"schemas"` |
| <a id="s-1181fd9fea"></a>`bundle_sha256` | `"720996a2b6d2582af728829cdb39132bd9136b723e7ded94fd859b99701b4862"` |
| <a id="s-a8b1495338"></a>`compatibility · contract_identity` | `"rfc8785-sha256"` |
| <a id="s-61caf561fe"></a>`compatibility · unknown_fields` | `"reject"` |
| <a id="s-cb84c5173d"></a>`compatibility · unknown_protocol_revision` | `"reject"` |
| <a id="s-b293a7b061"></a>`departure_effect · http_binding · operations · item 1 · error_schema` | `"ErrorOut"` |
| <a id="s-4486dd8543"></a>`departure_effect · http_binding · operations · item 1 · errors · item 1 · code` | `"bad_request"` |
| <a id="s-09c9978f0c"></a>`departure_effect · http_binding · operations · item 1 · errors · item 1 · status` | `400` |
| <a id="s-5a501e53b3"></a>`departure_effect · http_binding · operations · item 1 · errors · item 2 · code` | `"unauthorized"` |
| <a id="s-c2541afc8b"></a>`departure_effect · http_binding · operations · item 1 · errors · item 2 · status` | `401` |
| <a id="s-286dc5271b"></a>`departure_effect · http_binding · operations · item 1 · errors · item 3 · code` | `"target_failed"` |
| <a id="s-3cb9e7aae1"></a>`departure_effect · http_binding · operations · item 1 · errors · item 3 · status` | `500` |
| <a id="s-967e9532be"></a>`departure_effect · http_binding · operations · item 1 · method` | `"GET"` |
| <a id="s-b67c98b1a3"></a>`departure_effect · http_binding · operations · item 1 · path` | `"/v1/departure-target"` |
| <a id="s-9a4c17d14f"></a>`departure_effect · http_binding · operations · item 1 · path_parameters` | `[]` |
| <a id="s-80705dc6ee"></a>`departure_effect · http_binding · operations · item 1 · request · kind` | `"none"` |
| <a id="s-2987ad9a72"></a>`departure_effect · http_binding · operations · item 1 · request · schema` | `null` |
| <a id="s-e8ab6bec66"></a>`departure_effect · http_binding · operations · item 1 · response · headers` | `[]` |
| <a id="s-8561cb3e12"></a>`departure_effect · http_binding · operations · item 1 · response · kind` | `"json"` |
| <a id="s-cc53168f1c"></a>`departure_effect · http_binding · operations · item 1 · response · schema` | `"DepartureEffectTargetDescriptor"` |
| <a id="s-466ca11561"></a>`departure_effect · http_binding · operations · item 1 · response · statuses` | `[200]` |
| <a id="s-2b378849a3"></a>`departure_effect · http_binding · operations · item 2 · error_schema` | `"ErrorOut"` |
| <a id="s-519af9e909"></a>`departure_effect · http_binding · operations · item 2 · errors · item 1 · code` | `"invalid_target_request"` |
| <a id="s-0bdb71d849"></a>`departure_effect · http_binding · operations · item 2 · errors · item 1 · status` | `400` |
| <a id="s-44999cdcf9"></a>`departure_effect · http_binding · operations · item 2 · errors · item 2 · code` | `"unauthorized"` |
| <a id="s-8b85b22400"></a>`departure_effect · http_binding · operations · item 2 · errors · item 2 · status` | `401` |
| <a id="s-bcc2ec7d03"></a>`departure_effect · http_binding · operations · item 2 · errors · item 3 · code` | `"request_too_large"` |
| <a id="s-dfaeb0b7dc"></a>`departure_effect · http_binding · operations · item 2 · errors · item 3 · status` | `413` |
| <a id="s-fdfcc23e58"></a>`departure_effect · http_binding · operations · item 2 · errors · item 4 · code` | `"target_descriptor_mismatch"` |
| <a id="s-c078d06b3c"></a>`departure_effect · http_binding · operations · item 2 · errors · item 4 · status` | `409` |
| <a id="s-f8c98473bc"></a>`departure_effect · http_binding · operations · item 2 · errors · item 5 · code` | `"target_failed"` |
| <a id="s-034217900e"></a>`departure_effect · http_binding · operations · item 2 · errors · item 5 · status` | `500` |
| <a id="s-5f4eab5a21"></a>`departure_effect · http_binding · operations · item 2 · method` | `"PUT"` |
| <a id="s-fbfc17bb7c"></a>`departure_effect · http_binding · operations · item 2 · path` | `"/v1/departure-effects/{departure_id}"` |
| <a id="s-bf7c37ef10"></a>`departure_effect · http_binding · operations · item 2 · path_parameters · item 1 · name` | `"departure_id"` |
| <a id="s-ab9923229e"></a>`departure_effect · http_binding · operations · item 2 · path_parameters · item 1 · schema · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-613714f968"></a>`departure_effect · http_binding · operations · item 2 · path_parameters · item 1 · schema · type` | `"string"` |
| <a id="s-4a92b5f201"></a>`departure_effect · http_binding · operations · item 2 · request · kind` | `"json"` |
| <a id="s-6f8ab67375"></a>`departure_effect · http_binding · operations · item 2 · request · schema` | `"DepartureEffectIntent"` |
| <a id="s-5bd875c02b"></a>`departure_effect · http_binding · operations · item 2 · response · headers` | `[]` |
| <a id="s-18cd572b59"></a>`departure_effect · http_binding · operations · item 2 · response · kind` | `"json"` |
| <a id="s-f33854aa66"></a>`departure_effect · http_binding · operations · item 2 · response · schema` | `"DepartureEffectReceipt"` |
| <a id="s-8886381947"></a>`departure_effect · http_binding · operations · item 2 · response · statuses` | `[200]` |
| <a id="s-c8701af507"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · additionalProperties` | `false` |
| <a id="s-35e2d68112"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · maxLength` | `64` |
| <a id="s-809241b7e7"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · minLength` | `64` |
| <a id="s-9302621b5d"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-966c4d4688"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · title` | `"Archive Root Sha256"` |
| <a id="s-c879dc1483"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · type` | `"string"` |
| <a id="s-5d787d1be8"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · collection_id · $ref` | `"#/$defs/CollectionId"` |
| <a id="s-7d6cd6ce91"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · content_identity · maxLength` | `64` |
| <a id="s-68005b02b1"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · content_identity · minLength` | `64` |
| <a id="s-6dd82b49fa"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · content_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-6301a218f8"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · content_identity · title` | `"Content Identity"` |
| <a id="s-778a9f1141"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · content_identity · type` | `"string"` |
| <a id="s-3ce50a5cdd"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description · anyOf · item 1 · $ref` | `"#/$defs/CollectionDescription"` |
| <a id="s-ca1cad6960"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description · anyOf · item 2 · type` | `"null"` |
| <a id="s-c595903b67"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_identity · maxLength` | `64` |
| <a id="s-82d1cb34fc"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_identity · minLength` | `64` |
| <a id="s-e1d1a43b82"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-c4d5dc4380"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_identity · title` | `"Description Identity"` |
| <a id="s-cfffab3770"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_identity · type` | `"string"` |
| <a id="s-35af1e9101"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_revision · maximum` | `9007199254740991` |
| <a id="s-c76c72f1f9"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_revision · minimum` | `0` |
| <a id="s-9a5c2079ad"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_revision · title` | `"Description Revision"` |
| <a id="s-50c44f964b"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · description_revision · type` | `"integer"` |
| <a id="s-e79a7a51e6"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · revision · maxLength` | `19` |
| <a id="s-05667bef30"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · revision · minLength` | `1` |
| <a id="s-bbc36ca6ad"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · revision · pattern` | `"^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"` |
| <a id="s-09f38e001a"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · revision · title` | `"Revision"` |
| <a id="s-b0fde7b541"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · revision · type` | `"string"` |
| <a id="s-4f8944b08e"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_revision · maximum` | `9007199254740991` |
| <a id="s-cbf07206c2"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_revision · minimum` | `1` |
| <a id="s-0d388f8c14"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_revision · title` | `"Tag Revision"` |
| <a id="s-07e4995f02"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_revision · type` | `"integer"` |
| <a id="s-4970e9150b"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_set_identity · maxLength` | `64` |
| <a id="s-c951c9770a"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_set_identity · minLength` | `64` |
| <a id="s-f7a095ce7b"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_set_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-d18922faae"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_set_identity · title` | `"Tag Set Identity"` |
| <a id="s-fc0e688eb6"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · properties · tag_set_identity · type` | `"string"` |
| <a id="s-1738711608"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · required` | `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]` |
| <a id="s-1ff0f943b4"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · title` | `"CatalogSyncDescriptor"` |
| <a id="s-d940e27d6e"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CatalogSyncDescriptor · type` | `"object"` |
| <a id="s-62ed1ab95d"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionDescription · maxLength` | `32768` |
| <a id="s-f011df8d18"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionDescription · minLength` | `1` |
| <a id="s-90c2f411cb"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionDescription · type` | `"string"` |
| <a id="s-f7be007302"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionDescription · x-riverhog-encoded-bytes-max` | `32768` |
| <a id="s-02626b4ef3"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionDescription · x-riverhog-extent · policy` | `"contract_max"` |
| <a id="s-0cfccbb212"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionDescription · x-riverhog-extent · reason` | `"bounded-human-authored-catalog-description"` |
| <a id="s-d3f5d0f78e"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionDescription · x-unicode-normalization` | `"NFC"` |
| <a id="s-688939cfa4"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionId · allOf · item 1 · pattern` | `"^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"` |
| <a id="s-3b88c1cbe7"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionId · allOf · item 1 · type` | `"string"` |
| <a id="s-1a1683741d"></a>`departure_effect · schemas · DepartureEffectIntent · $defs · CollectionId · allOf · item 2 · not · const` | `"0"` |
| <a id="s-9046f8f67c"></a>`departure_effect · schemas · DepartureEffectIntent · additionalProperties` | `false` |
| <a id="s-dfcb2c9a2d"></a>`departure_effect · schemas · DepartureEffectIntent · properties · authorization_view_identity · maxLength` | `64` |
| <a id="s-41c5ec2905"></a>`departure_effect · schemas · DepartureEffectIntent · properties · authorization_view_identity · minLength` | `64` |
| <a id="s-3fcca31e4c"></a>`departure_effect · schemas · DepartureEffectIntent · properties · authorization_view_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-f281332e9b"></a>`departure_effect · schemas · DepartureEffectIntent · properties · authorization_view_identity · title` | `"Authorization View Identity"` |
| <a id="s-dfa6f64994"></a>`departure_effect · schemas · DepartureEffectIntent · properties · authorization_view_identity · type` | `"string"` |
| <a id="s-ff936280d6"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_cause · enum` | `["collection_deleted","visibility_lost"]` |
| <a id="s-80f6f49548"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_cause · title` | `"Departure Cause"` |
| <a id="s-cf34449b54"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_cause · type` | `"string"` |
| <a id="s-929534e77d"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_id · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-598e8f5548"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_id · title` | `"Departure Id"` |
| <a id="s-b56c4fd9bb"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_id · type` | `"string"` |
| <a id="s-1dd877414c"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_revision · maxLength` | `19` |
| <a id="s-0c7cd00035"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_revision · minLength` | `1` |
| <a id="s-4a71f1008c"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_revision · pattern` | `"^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"` |
| <a id="s-05ac797726"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_revision · title` | `"Departure Revision"` |
| <a id="s-ae5e637e58"></a>`departure_effect · schemas · DepartureEffectIntent · properties · departure_revision · type` | `"string"` |
| <a id="s-f8528fd8ac"></a>`departure_effect · schemas · DepartureEffectIntent · properties · format · const` | `"stove0-departure-effect-intent/v1"` |
| <a id="s-02fbb79c21"></a>`departure_effect · schemas · DepartureEffectIntent · properties · format · default` | `"stove0-departure-effect-intent/v1"` |
| <a id="s-c54462f373"></a>`departure_effect · schemas · DepartureEffectIntent · properties · format · title` | `"Format"` |
| <a id="s-1187524280"></a>`departure_effect · schemas · DepartureEffectIntent · properties · format · type` | `"string"` |
| <a id="s-b2359e9438"></a>`departure_effect · schemas · DepartureEffectIntent · properties · last_collection · $ref` | `"#/$defs/CatalogSyncDescriptor"` |
| <a id="s-7308ece1e0"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_id · pattern` | `"^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"` |
| <a id="s-1485cee28d"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_id · title` | `"Policy Id"` |
| <a id="s-de6af06764"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_id · type` | `"string"` |
| <a id="s-ef7a428cec"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_revision · minimum` | `1` |
| <a id="s-67bc5b9816"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_revision · title` | `"Policy Revision"` |
| <a id="s-3338afc068"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_revision · type` | `"integer"` |
| <a id="s-e0814165a3"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_sha256 · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-ce00cc865f"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_sha256 · title` | `"Policy Sha256"` |
| <a id="s-8af8abcd30"></a>`departure_effect · schemas · DepartureEffectIntent · properties · policy_sha256 · type` | `"string"` |
| <a id="s-93d0de63f0"></a>`departure_effect · schemas · DepartureEffectIntent · properties · source_identity · maxLength` | `64` |
| <a id="s-9e92675ee8"></a>`departure_effect · schemas · DepartureEffectIntent · properties · source_identity · minLength` | `64` |
| <a id="s-1889ef288b"></a>`departure_effect · schemas · DepartureEffectIntent · properties · source_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-178f77146d"></a>`departure_effect · schemas · DepartureEffectIntent · properties · source_identity · title` | `"Source Identity"` |
| <a id="s-46e908ebbd"></a>`departure_effect · schemas · DepartureEffectIntent · properties · source_identity · type` | `"string"` |
| <a id="s-dc451c0a1c"></a>`departure_effect · schemas · DepartureEffectIntent · properties · target_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-91808783c2"></a>`departure_effect · schemas · DepartureEffectIntent · properties · target_identity · title` | `"Target Identity"` |
| <a id="s-3811f1448b"></a>`departure_effect · schemas · DepartureEffectIntent · properties · target_identity · type` | `"string"` |
| <a id="s-0206173605"></a>`departure_effect · schemas · DepartureEffectIntent · properties · target_registration_id · maxLength` | `160` |
| <a id="s-7637b3361d"></a>`departure_effect · schemas · DepartureEffectIntent · properties · target_registration_id · minLength` | `1` |
| <a id="s-b68d99d1d2"></a>`departure_effect · schemas · DepartureEffectIntent · properties · target_registration_id · title` | `"Target Registration Id"` |
| <a id="s-e14f65aaa3"></a>`departure_effect · schemas · DepartureEffectIntent · properties · target_registration_id · type` | `"string"` |
| <a id="s-885baa121b"></a>`departure_effect · schemas · DepartureEffectIntent · required` | `["policy_id","policy_revision","policy_sha256","target_registration_id","target_identity","source_identity","authorization_view_identity","last_collection","departure_cause","departure_revision","departure_id"]` |
| <a id="s-e67b33163f"></a>`departure_effect · schemas · DepartureEffectIntent · title` | `"DepartureEffectIntent"` |
| <a id="s-e5448b1fd1"></a>`departure_effect · schemas · DepartureEffectIntent · type` | `"object"` |
| <a id="s-70d2630e80"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · additionalProperties` | `false` |
| <a id="s-991bc88ed2"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · maxLength` | `64` |
| <a id="s-5b82e31f50"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · minLength` | `64` |
| <a id="s-ff4a4cf5e4"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-beeb878faf"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · title` | `"Archive Root Sha256"` |
| <a id="s-6a3f6471f6"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · archive_root_sha256 · type` | `"string"` |
| <a id="s-4158c2e782"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · collection_id · $ref` | `"#/$defs/CollectionId"` |
| <a id="s-33b3bc99b6"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · content_identity · maxLength` | `64` |
| <a id="s-caa6daae14"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · content_identity · minLength` | `64` |
| <a id="s-214fda095f"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · content_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-60a4619394"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · content_identity · title` | `"Content Identity"` |
| <a id="s-0301743caa"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · content_identity · type` | `"string"` |
| <a id="s-1a6a4e9b8b"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description · anyOf · item 1 · $ref` | `"#/$defs/CollectionDescription"` |
| <a id="s-ad1307fe31"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description · anyOf · item 2 · type` | `"null"` |
| <a id="s-a671fcb68b"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_identity · maxLength` | `64` |
| <a id="s-8137c437f7"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_identity · minLength` | `64` |
| <a id="s-d81722eb00"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-e4f951816d"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_identity · title` | `"Description Identity"` |
| <a id="s-83dd100566"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_identity · type` | `"string"` |
| <a id="s-14cbbd8e5f"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_revision · maximum` | `9007199254740991` |
| <a id="s-7f08c61473"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_revision · minimum` | `0` |
| <a id="s-61f215ef20"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_revision · title` | `"Description Revision"` |
| <a id="s-37af4de0f7"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · description_revision · type` | `"integer"` |
| <a id="s-0d3e86a224"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · revision · maxLength` | `19` |
| <a id="s-368fc0cee8"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · revision · minLength` | `1` |
| <a id="s-fdc2b30fa6"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · revision · pattern` | `"^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"` |
| <a id="s-b116e8fda9"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · revision · title` | `"Revision"` |
| <a id="s-b5e07d6dad"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · revision · type` | `"string"` |
| <a id="s-09200de492"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_revision · maximum` | `9007199254740991` |
| <a id="s-981e6504cb"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_revision · minimum` | `1` |
| <a id="s-d8d4bf91c2"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_revision · title` | `"Tag Revision"` |
| <a id="s-88b88b1a85"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_revision · type` | `"integer"` |
| <a id="s-83908f41d8"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_set_identity · maxLength` | `64` |
| <a id="s-c7227f27d0"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_set_identity · minLength` | `64` |
| <a id="s-224b214aa3"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_set_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-47f3504f7c"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_set_identity · title` | `"Tag Set Identity"` |
| <a id="s-31b3a8d1cb"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · properties · tag_set_identity · type` | `"string"` |
| <a id="s-199f29d629"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · required` | `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]` |
| <a id="s-3cad132085"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · title` | `"CatalogSyncDescriptor"` |
| <a id="s-0323c6983b"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CatalogSyncDescriptor · type` | `"object"` |
| <a id="s-e0341c47e9"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionDescription · maxLength` | `32768` |
| <a id="s-1ed6afaa86"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionDescription · minLength` | `1` |
| <a id="s-90320da3e6"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionDescription · type` | `"string"` |
| <a id="s-4c18eb21c5"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionDescription · x-riverhog-encoded-bytes-max` | `32768` |
| <a id="s-853ac2c2a4"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionDescription · x-riverhog-extent · policy` | `"contract_max"` |
| <a id="s-a469ae01e8"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionDescription · x-riverhog-extent · reason` | `"bounded-human-authored-catalog-description"` |
| <a id="s-c82468b8d8"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionDescription · x-unicode-normalization` | `"NFC"` |
| <a id="s-6c3e5ae833"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionId · allOf · item 1 · pattern` | `"^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])"` |
| <a id="s-9e01e12135"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionId · allOf · item 1 · type` | `"string"` |
| <a id="s-79a717cbb9"></a>`departure_effect · schemas · DepartureEffectIntentPayload · $defs · CollectionId · allOf · item 2 · not · const` | `"0"` |
| <a id="s-9abe6aa59a"></a>`departure_effect · schemas · DepartureEffectIntentPayload · additionalProperties` | `false` |
| <a id="s-a575fbfc90"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · authorization_view_identity · maxLength` | `64` |
| <a id="s-494777e3bb"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · authorization_view_identity · minLength` | `64` |
| <a id="s-669b143769"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · authorization_view_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-60aa00ae8f"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · authorization_view_identity · title` | `"Authorization View Identity"` |
| <a id="s-227af4990d"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · authorization_view_identity · type` | `"string"` |
| <a id="s-d9237f08fd"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_cause · enum` | `["collection_deleted","visibility_lost"]` |
| <a id="s-02a2a59747"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_cause · title` | `"Departure Cause"` |
| <a id="s-7a7372e565"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_cause · type` | `"string"` |
| <a id="s-9613dd9d1d"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_revision · maxLength` | `19` |
| <a id="s-ab71e1f099"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_revision · minLength` | `1` |
| <a id="s-de41724cd9"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_revision · pattern` | `"^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"` |
| <a id="s-19bb92efa9"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_revision · title` | `"Departure Revision"` |
| <a id="s-31eb898ee8"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · departure_revision · type` | `"string"` |
| <a id="s-426fb26426"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · format · const` | `"stove0-departure-effect-intent/v1"` |
| <a id="s-6054bc0888"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · format · default` | `"stove0-departure-effect-intent/v1"` |
| <a id="s-b67b057f11"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · format · title` | `"Format"` |
| <a id="s-8c95c3674d"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · format · type` | `"string"` |
| <a id="s-1209d9325b"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · last_collection · $ref` | `"#/$defs/CatalogSyncDescriptor"` |
| <a id="s-a94f5bdaa9"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_id · pattern` | `"^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"` |
| <a id="s-91358b69c4"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_id · title` | `"Policy Id"` |
| <a id="s-22e37a28e2"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_id · type` | `"string"` |
| <a id="s-dca1dff2b6"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_revision · minimum` | `1` |
| <a id="s-0466d9e14a"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_revision · title` | `"Policy Revision"` |
| <a id="s-cab0a9b006"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_revision · type` | `"integer"` |
| <a id="s-11c7fd9b01"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_sha256 · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-89f7d10aaf"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_sha256 · title` | `"Policy Sha256"` |
| <a id="s-0fd5dc83e4"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · policy_sha256 · type` | `"string"` |
| <a id="s-da2635a3e3"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · source_identity · maxLength` | `64` |
| <a id="s-ce5d232812"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · source_identity · minLength` | `64` |
| <a id="s-3e0dc9f3bb"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · source_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-c83ee56dc6"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · source_identity · title` | `"Source Identity"` |
| <a id="s-dc4f77c27f"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · source_identity · type` | `"string"` |
| <a id="s-730cbff2dc"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · target_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-c7d3704846"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · target_identity · title` | `"Target Identity"` |
| <a id="s-b9f900bf0f"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · target_identity · type` | `"string"` |
| <a id="s-8965878888"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · target_registration_id · maxLength` | `160` |
| <a id="s-688e19882b"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · target_registration_id · minLength` | `1` |
| <a id="s-a185c22108"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · target_registration_id · title` | `"Target Registration Id"` |
| <a id="s-76e3014a33"></a>`departure_effect · schemas · DepartureEffectIntentPayload · properties · target_registration_id · type` | `"string"` |
| <a id="s-8f91bc043b"></a>`departure_effect · schemas · DepartureEffectIntentPayload · required` | `["policy_id","policy_revision","policy_sha256","target_registration_id","target_identity","source_identity","authorization_view_identity","last_collection","departure_cause","departure_revision"]` |
| <a id="s-b24c7ee1f1"></a>`departure_effect · schemas · DepartureEffectIntentPayload · title` | `"DepartureEffectIntentPayload"` |
| <a id="s-78d7fede6e"></a>`departure_effect · schemas · DepartureEffectIntentPayload · type` | `"object"` |
| <a id="s-e266a4bcfe"></a>`departure_effect · schemas · DepartureEffectReceipt · $defs · JsonValue` | `{}` |
| <a id="s-440583eb2b"></a>`departure_effect · schemas · DepartureEffectReceipt · additionalProperties` | `false` |
| <a id="s-2c06f8f09a"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · departure_id · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-065842a7ef"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · departure_id · title` | `"Departure Id"` |
| <a id="s-b457d91a87"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · departure_id · type` | `"string"` |
| <a id="s-246e4bbcdb"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · format · const` | `"stove0-departure-effect-receipt/v1"` |
| <a id="s-26d259793d"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · format · default` | `"stove0-departure-effect-receipt/v1"` |
| <a id="s-2b68a9ed18"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · format · title` | `"Format"` |
| <a id="s-e971d0b8b9"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · format · type` | `"string"` |
| <a id="s-c35ee129b3"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · receipt_sha256 · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-67407fa34f"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · receipt_sha256 · title` | `"Receipt Sha256"` |
| <a id="s-3d24e2a110"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · receipt_sha256 · type` | `"string"` |
| <a id="s-b5008f1268"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · result · additionalProperties · $ref` | `"#/$defs/JsonValue"` |
| <a id="s-2a709f0148"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · result · title` | `"Result"` |
| <a id="s-35accc8789"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · result · type` | `"object"` |
| <a id="s-b6305cc7f5"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · result · x-riverhog-encoded-bytes-max` | `65536` |
| <a id="s-b54cea5cfe"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · result · x-riverhog-extent · policy` | `"contract_max"` |
| <a id="s-62fa8fbb5c"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · result · x-riverhog-extent · reason` | `"bounded-departure-effect-receipt"` |
| <a id="s-92db2254ed"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · target_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-2df305d724"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · target_identity · title` | `"Target Identity"` |
| <a id="s-b84bc71f36"></a>`departure_effect · schemas · DepartureEffectReceipt · properties · target_identity · type` | `"string"` |
| <a id="s-6d260977f0"></a>`departure_effect · schemas · DepartureEffectReceipt · required` | `["departure_id","target_identity","result","receipt_sha256"]` |
| <a id="s-6875440633"></a>`departure_effect · schemas · DepartureEffectReceipt · title` | `"DepartureEffectReceipt"` |
| <a id="s-878b280aa9"></a>`departure_effect · schemas · DepartureEffectReceipt · type` | `"object"` |
| <a id="s-c6cf53f09f"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · $defs · JsonValue` | `{}` |
| <a id="s-f5135f359f"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · additionalProperties` | `false` |
| <a id="s-191cef240e"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · departure_id · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-e9f95aec48"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · departure_id · title` | `"Departure Id"` |
| <a id="s-699966a99e"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · departure_id · type` | `"string"` |
| <a id="s-3b88ac06e6"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · format · const` | `"stove0-departure-effect-receipt/v1"` |
| <a id="s-e6b8f05714"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · format · default` | `"stove0-departure-effect-receipt/v1"` |
| <a id="s-c727380c61"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · format · title` | `"Format"` |
| <a id="s-9e85363b10"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · format · type` | `"string"` |
| <a id="s-17d389a784"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · result · additionalProperties · $ref` | `"#/$defs/JsonValue"` |
| <a id="s-baa32ba2fa"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · result · title` | `"Result"` |
| <a id="s-243ad000f9"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · result · type` | `"object"` |
| <a id="s-7955b95e76"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · result · x-riverhog-encoded-bytes-max` | `65536` |
| <a id="s-508b344ac3"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · result · x-riverhog-extent · policy` | `"contract_max"` |
| <a id="s-3d1f62be4d"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · result · x-riverhog-extent · reason` | `"bounded-departure-effect-receipt"` |
| <a id="s-8c4b69ca42"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · target_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-ed71bb578a"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · target_identity · title` | `"Target Identity"` |
| <a id="s-0aa94a3a7d"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · properties · target_identity · type` | `"string"` |
| <a id="s-39752d59a3"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · required` | `["departure_id","target_identity","result"]` |
| <a id="s-3da45a9873"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · title` | `"DepartureEffectReceiptPayload"` |
| <a id="s-82e6a2dabb"></a>`departure_effect · schemas · DepartureEffectReceiptPayload · type` | `"object"` |
| <a id="s-69ac2e789e"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · additionalProperties` | `false` |
| <a id="s-5adca13d46"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · format · const` | `"stove0-departure-effect-target/v1"` |
| <a id="s-8826408c70"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · format · default` | `"stove0-departure-effect-target/v1"` |
| <a id="s-1c73f83db7"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · format · title` | `"Format"` |
| <a id="s-c305bc478a"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · format · type` | `"string"` |
| <a id="s-3a97e1ef89"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · image_id · pattern` | `"^sha256:[0-9a-f]{64}$"` |
| <a id="s-ae88b3d668"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · image_id · title` | `"Image Id"` |
| <a id="s-852504c7f6"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · image_id · type` | `"string"` |
| <a id="s-f5eb08e6f4"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · implementation_id · pattern` | `"^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"` |
| <a id="s-5806e19f0e"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · implementation_id · title` | `"Implementation Id"` |
| <a id="s-30cc86c8cd"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · implementation_id · type` | `"string"` |
| <a id="s-2d6c1ae4cf"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · implementation_version · maxLength` | `120` |
| <a id="s-66690a4c42"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · implementation_version · minLength` | `1` |
| <a id="s-38d3e5f1b8"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · implementation_version · title` | `"Implementation Version"` |
| <a id="s-38865e6126"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · implementation_version · type` | `"string"` |
| <a id="s-8d568f1cff"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · scope_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-f791c8cbdc"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · scope_identity · title` | `"Scope Identity"` |
| <a id="s-0898a3d438"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · scope_identity · type` | `"string"` |
| <a id="s-f7fdee7ec0"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · source_revision · maxLength` | `200` |
| <a id="s-d730299428"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · source_revision · minLength` | `1` |
| <a id="s-e4d61e6718"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · source_revision · title` | `"Source Revision"` |
| <a id="s-6d29a25af3"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · source_revision · type` | `"string"` |
| <a id="s-966859dab0"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · target_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-aceea44be7"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · target_identity · title` | `"Target Identity"` |
| <a id="s-e6d594f30f"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · properties · target_identity · type` | `"string"` |
| <a id="s-9a5ff713cd"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · required` | `["implementation_id","implementation_version","source_revision","image_id","scope_identity","target_identity"]` |
| <a id="s-097d7fae86"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · title` | `"DepartureEffectTargetDescriptor"` |
| <a id="s-2f60b7d975"></a>`departure_effect · schemas · DepartureEffectTargetDescriptor · type` | `"object"` |
| <a id="s-51168a8173"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · additionalProperties` | `false` |
| <a id="s-356cb46ad9"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · format · const` | `"stove0-departure-effect-target/v1"` |
| <a id="s-50ca7ab4ba"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · format · default` | `"stove0-departure-effect-target/v1"` |
| <a id="s-316f425450"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · format · title` | `"Format"` |
| <a id="s-732ff0e5dd"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · format · type` | `"string"` |
| <a id="s-d8f02cb93e"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · image_id · pattern` | `"^sha256:[0-9a-f]{64}$"` |
| <a id="s-f8c6fce49a"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · image_id · title` | `"Image Id"` |
| <a id="s-3f5b97ccbe"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · image_id · type` | `"string"` |
| <a id="s-ada2cf6d5e"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · implementation_id · pattern` | `"^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"` |
| <a id="s-7b6ec3c57a"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · implementation_id · title` | `"Implementation Id"` |
| <a id="s-3c7ee8d641"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · implementation_id · type` | `"string"` |
| <a id="s-f1f022efe4"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · implementation_version · maxLength` | `120` |
| <a id="s-ddb47500af"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · implementation_version · minLength` | `1` |
| <a id="s-cb3c4450b7"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · implementation_version · title` | `"Implementation Version"` |
| <a id="s-9ca7353de2"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · implementation_version · type` | `"string"` |
| <a id="s-d152fe5998"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · scope_identity · pattern` | `"^[0-9a-f]{64}$"` |
| <a id="s-7f75861be2"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · scope_identity · title` | `"Scope Identity"` |
| <a id="s-2ad1d991c4"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · scope_identity · type` | `"string"` |
| <a id="s-72069a2220"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · source_revision · maxLength` | `200` |
| <a id="s-b75a8c3378"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · source_revision · minLength` | `1` |
| <a id="s-1b95ff2429"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · source_revision · title` | `"Source Revision"` |
| <a id="s-2ba76919ac"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · properties · source_revision · type` | `"string"` |
| <a id="s-802814d58a"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · required` | `["implementation_id","implementation_version","source_revision","image_id","scope_identity"]` |
| <a id="s-0f5614956d"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · title` | `"DepartureEffectTargetDescriptorPayload"` |
| <a id="s-5b57cb5231"></a>`departure_effect · schemas · DepartureEffectTargetDescriptorPayload · type` | `"object"` |
| <a id="s-ced58bf27d"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · additionalProperties` | `false` |
| <a id="s-c68a65a783"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · code · minLength` | `1` |
| <a id="s-99a34c2e35"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · code · title` | `"Code"` |
| <a id="s-e12d28188e"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · code · type` | `"string"` |
| <a id="s-93a1e59e32"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · details · anyOf · item 1 · additionalProperties` | `true` |
| <a id="s-800b6d5fcb"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · details · anyOf · item 1 · type` | `"object"` |
| <a id="s-73247499a4"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · details · anyOf · item 2 · type` | `"null"` |
| <a id="s-f581c069f1"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · details · default` | `null` |
| <a id="s-a4e40764e0"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · details · title` | `"Details"` |
| <a id="s-8a1a1028a6"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · message · minLength` | `1` |
| <a id="s-c7a91d5fc2"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · message · title` | `"Message"` |
| <a id="s-7d9ee7f35e"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · properties · message · type` | `"string"` |
| <a id="s-0b6a350a49"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · required` | `["code","message"]` |
| <a id="s-cbb0135d92"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · title` | `"ErrorBody"` |
| <a id="s-f015ab1a99"></a>`departure_effect · schemas · ErrorOut · $defs · ErrorBody · type` | `"object"` |
| <a id="s-88ff549204"></a>`departure_effect · schemas · ErrorOut · additionalProperties` | `false` |
| <a id="s-0068291d16"></a>`departure_effect · schemas · ErrorOut · properties · error · $ref` | `"#/$defs/ErrorBody"` |
| <a id="s-a3f6109310"></a>`departure_effect · schemas · ErrorOut · required` | `["error"]` |
| <a id="s-f8e55c6793"></a>`departure_effect · schemas · ErrorOut · title` | `"ErrorOut"` |
| <a id="s-40c3afb8d6"></a>`departure_effect · schemas · ErrorOut · type` | `"object"` |
| <a id="s-e51e46fd84"></a>`format` | `"stove0-target-schema-bundle/v1"` |
| <a id="s-aeb93f5dc6"></a>`protocols` | `["stove0-transform-target/v1","stove0-effect-target/v1"]` |
| <a id="s-f06050ba6c"></a>`semantic_acceptance · binding` | `"OperationContract.intent_semantics"` |
| <a id="s-476e256aa7"></a>`semantic_acceptance · identity` | `["id","profile_sha256"]` |
| <a id="s-868e8c2ec7"></a>`semantic_acceptance · kind` | `"operation-contract"` |
| <a id="s-0543785f8d"></a>`semantic_acceptance · request_response_relations` | `"required"` |

## Maintained corroboration

### Related interface records

- [GET /v1/jobs/{job_id}](../process-protocol-operations/get-v1-jobs-job-id.md)
- [GET /v1/target](../process-protocol-operations/get-v1-target.md)
- [POST /v1/jobs/{job_id}/cancel](../process-protocol-operations/post-v1-jobs-job-id-cancel.md)
- [POST /v1/preflight](../process-protocol-operations/post-v1-preflight.md)
- [PUT /v1/jobs/{job_id}](../process-protocol-operations/put-v1-jobs-job-id.md)
- [generated:stove0-target: ErrorOut](../process-protocol-schemas/generated-stove0-target-errorout.md)
- [generated:stove0-target: OperationContract](../process-protocol-schemas/generated-stove0-target-operationcontract.md)
- [generated:stove0-target: TargetConformanceResult](../process-protocol-schemas/generated-stove0-target-targetconformanceresult.md)
- [generated:stove0-target: TargetDescriptor](../process-protocol-schemas/generated-stove0-target-targetdescriptor.md)
- [generated:stove0-target: TargetJobRequest](../process-protocol-schemas/generated-stove0-target-targetjobrequest.md)
- [generated:stove0-target: TargetJobStatus](../process-protocol-schemas/generated-stove0-target-targetjobstatus.md)
- [generated:stove0-target: TargetPreflightRequest](../process-protocol-schemas/generated-stove0-target-targetpreflightrequest.md)
- [generated:stove0-target: TargetPreflightResponse](../process-protocol-schemas/generated-stove0-target-targetpreflightresponse.md)

## Governing policies

- <a id="pa-d9bfc8e169"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:stove0-target](../../../evidence/sources/authorities.md#src-2c42f9d39a) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/schemas.py::target\_schema\_bundle](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/authorities`
- `/external_contract/protocol_schemas/generated:stove0-target/bundle_sha256`
- `/external_contract/protocol_schemas/generated:stove0-target/compatibility`
- `/external_contract/protocol_schemas/generated:stove0-target/departure_effect`
- `/external_contract/protocol_schemas/generated:stove0-target/format`
- `/external_contract/protocol_schemas/generated:stove0-target/protocols`
- `/external_contract/protocol_schemas/generated:stove0-target/semantic_acceptance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:stove0-target/authorities`

<!-- exact-contract-value: 7f69084a56b6669babee85db1d55010b20bebfd7b2307a3660b8e24e89f5d153 -->

```json
{
  "departure_http_operations": "departure_effect.http_binding.operations",
  "departure_structural_models": "departure_effect.schemas",
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/bundle_sha256`

<!-- exact-contract-value: dc8064a6800abd3b3b21e05595116ee3423066e8f3242750828e538333ebc74a -->

```json
"720996a2b6d2582af728829cdb39132bd9136b723e7ded94fd859b99701b4862"
```

### `/external_contract/protocol_schemas/generated:stove0-target/compatibility`

<!-- exact-contract-value: fb2ae5072cf53b5181c923503273657248116b12ab23daed566f2fd061e53e27 -->

```json
{
  "contract_identity": "rfc8785-sha256",
  "unknown_fields": "reject",
  "unknown_protocol_revision": "reject"
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/departure_effect`

<!-- exact-contract-value: cd1e69c6bfd84aaf21136548ac759df8903ef4334336548b3502b386071f9b45 -->

```json
{
  "http_binding": {
    "operations": [
      {
        "error_schema": "ErrorOut",
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
            "code": "target_failed",
            "status": 500
          }
        ],
        "method": "GET",
        "path": "/v1/departure-target",
        "path_parameters": [],
        "request": {
          "kind": "none",
          "schema": null
        },
        "response": {
          "headers": [],
          "kind": "json",
          "schema": "DepartureEffectTargetDescriptor",
          "statuses": [
            200
          ]
        }
      },
      {
        "error_schema": "ErrorOut",
        "errors": [
          {
            "code": "invalid_target_request",
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
            "code": "target_descriptor_mismatch",
            "status": 409
          },
          {
            "code": "target_failed",
            "status": 500
          }
        ],
        "method": "PUT",
        "path": "/v1/departure-effects/{departure_id}",
        "path_parameters": [
          {
            "name": "departure_id",
            "schema": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          }
        ],
        "request": {
          "kind": "json",
          "schema": "DepartureEffectIntent"
        },
        "response": {
          "headers": [],
          "kind": "json",
          "schema": "DepartureEffectReceipt",
          "statuses": [
            200
          ]
        }
      }
    ]
  },
  "schemas": {
    "DepartureEffectIntent": {
      "$defs": {
        "CatalogSyncDescriptor": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            },
            "description": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionDescription"
                },
                {
                  "type": "null"
                }
              ]
            },
            "description_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Description Identity",
              "type": "string"
            },
            "description_revision": {
              "maximum": 9007199254740991,
              "minimum": 0,
              "title": "Description Revision",
              "type": "integer"
            },
            "revision": {
              "maxLength": 19,
              "minLength": 1,
              "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
              "title": "Revision",
              "type": "string"
            },
            "tag_revision": {
              "maximum": 9007199254740991,
              "minimum": 1,
              "title": "Tag Revision",
              "type": "integer"
            },
            "tag_set_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Tag Set Identity",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "description",
            "description_revision",
            "description_identity",
            "tag_revision",
            "tag_set_identity",
            "revision"
          ],
          "title": "CatalogSyncDescriptor",
          "type": "object"
        },
        "CollectionDescription": {
          "maxLength": 32768,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 32768,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-catalog-description"
          },
          "x-unicode-normalization": "NFC"
        },
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        }
      },
      "additionalProperties": false,
      "properties": {
        "authorization_view_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "title": "Authorization View Identity",
          "type": "string"
        },
        "departure_cause": {
          "enum": [
            "collection_deleted",
            "visibility_lost"
          ],
          "title": "Departure Cause",
          "type": "string"
        },
        "departure_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Departure Id",
          "type": "string"
        },
        "departure_revision": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
          "title": "Departure Revision",
          "type": "string"
        },
        "format": {
          "const": "stove0-departure-effect-intent/v1",
          "default": "stove0-departure-effect-intent/v1",
          "title": "Format",
          "type": "string"
        },
        "last_collection": {
          "$ref": "#/$defs/CatalogSyncDescriptor"
        },
        "policy_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
          "title": "Policy Id",
          "type": "string"
        },
        "policy_revision": {
          "minimum": 1,
          "title": "Policy Revision",
          "type": "integer"
        },
        "policy_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Policy Sha256",
          "type": "string"
        },
        "source_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "title": "Source Identity",
          "type": "string"
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Identity",
          "type": "string"
        },
        "target_registration_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Target Registration Id",
          "type": "string"
        }
      },
      "required": [
        "policy_id",
        "policy_revision",
        "policy_sha256",
        "target_registration_id",
        "target_identity",
        "source_identity",
        "authorization_view_identity",
        "last_collection",
        "departure_cause",
        "departure_revision",
        "departure_id"
      ],
      "title": "DepartureEffectIntent",
      "type": "object"
    },
    "DepartureEffectIntentPayload": {
      "$defs": {
        "CatalogSyncDescriptor": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            },
            "description": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionDescription"
                },
                {
                  "type": "null"
                }
              ]
            },
            "description_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Description Identity",
              "type": "string"
            },
            "description_revision": {
              "maximum": 9007199254740991,
              "minimum": 0,
              "title": "Description Revision",
              "type": "integer"
            },
            "revision": {
              "maxLength": 19,
              "minLength": 1,
              "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
              "title": "Revision",
              "type": "string"
            },
            "tag_revision": {
              "maximum": 9007199254740991,
              "minimum": 1,
              "title": "Tag Revision",
              "type": "integer"
            },
            "tag_set_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "title": "Tag Set Identity",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "description",
            "description_revision",
            "description_identity",
            "tag_revision",
            "tag_set_identity",
            "revision"
          ],
          "title": "CatalogSyncDescriptor",
          "type": "object"
        },
        "CollectionDescription": {
          "maxLength": 32768,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 32768,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-catalog-description"
          },
          "x-unicode-normalization": "NFC"
        },
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        }
      },
      "additionalProperties": false,
      "properties": {
        "authorization_view_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "title": "Authorization View Identity",
          "type": "string"
        },
        "departure_cause": {
          "enum": [
            "collection_deleted",
            "visibility_lost"
          ],
          "title": "Departure Cause",
          "type": "string"
        },
        "departure_revision": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
          "title": "Departure Revision",
          "type": "string"
        },
        "format": {
          "const": "stove0-departure-effect-intent/v1",
          "default": "stove0-departure-effect-intent/v1",
          "title": "Format",
          "type": "string"
        },
        "last_collection": {
          "$ref": "#/$defs/CatalogSyncDescriptor"
        },
        "policy_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
          "title": "Policy Id",
          "type": "string"
        },
        "policy_revision": {
          "minimum": 1,
          "title": "Policy Revision",
          "type": "integer"
        },
        "policy_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Policy Sha256",
          "type": "string"
        },
        "source_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "title": "Source Identity",
          "type": "string"
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Identity",
          "type": "string"
        },
        "target_registration_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Target Registration Id",
          "type": "string"
        }
      },
      "required": [
        "policy_id",
        "policy_revision",
        "policy_sha256",
        "target_registration_id",
        "target_identity",
        "source_identity",
        "authorization_view_identity",
        "last_collection",
        "departure_cause",
        "departure_revision"
      ],
      "title": "DepartureEffectIntentPayload",
      "type": "object"
    },
    "DepartureEffectReceipt": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "departure_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Departure Id",
          "type": "string"
        },
        "format": {
          "const": "stove0-departure-effect-receipt/v1",
          "default": "stove0-departure-effect-receipt/v1",
          "title": "Format",
          "type": "string"
        },
        "receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Receipt Sha256",
          "type": "string"
        },
        "result": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Result",
          "type": "object",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-departure-effect-receipt"
          }
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Identity",
          "type": "string"
        }
      },
      "required": [
        "departure_id",
        "target_identity",
        "result",
        "receipt_sha256"
      ],
      "title": "DepartureEffectReceipt",
      "type": "object"
    },
    "DepartureEffectReceiptPayload": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "departure_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Departure Id",
          "type": "string"
        },
        "format": {
          "const": "stove0-departure-effect-receipt/v1",
          "default": "stove0-departure-effect-receipt/v1",
          "title": "Format",
          "type": "string"
        },
        "result": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Result",
          "type": "object",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-departure-effect-receipt"
          }
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Identity",
          "type": "string"
        }
      },
      "required": [
        "departure_id",
        "target_identity",
        "result"
      ],
      "title": "DepartureEffectReceiptPayload",
      "type": "object"
    },
    "DepartureEffectTargetDescriptor": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-departure-effect-target/v1",
          "default": "stove0-departure-effect-target/v1",
          "title": "Format",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
          "title": "Image Id",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "scope_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Scope Identity",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Identity",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_id",
        "scope_identity",
        "target_identity"
      ],
      "title": "DepartureEffectTargetDescriptor",
      "type": "object"
    },
    "DepartureEffectTargetDescriptorPayload": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-departure-effect-target/v1",
          "default": "stove0-departure-effect-target/v1",
          "title": "Format",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
          "title": "Image Id",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "scope_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Scope Identity",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_id",
        "scope_identity"
      ],
      "title": "DepartureEffectTargetDescriptorPayload",
      "type": "object"
    },
    "ErrorOut": {
      "$defs": {
        "ErrorBody": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "minLength": 1,
              "title": "Code",
              "type": "string"
            },
            "details": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Details"
            },
            "message": {
              "minLength": 1,
              "title": "Message",
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "title": "ErrorBody",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "error": {
          "$ref": "#/$defs/ErrorBody"
        }
      },
      "required": [
        "error"
      ],
      "title": "ErrorOut",
      "type": "object"
    }
  }
}
```

### `/external_contract/protocol_schemas/generated:stove0-target/format`

<!-- exact-contract-value: 6844917b9246b56594c9d97f148456b7e55cc6a0d13cb3cad09d1d8af02666c7 -->

```json
"stove0-target-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:stove0-target/protocols`

<!-- exact-contract-value: 6fa34737ad43c7376f68219f2deb607e97cb65bd84cd3d10c9372dd01ddb8d77 -->

```json
[
  "stove0-transform-target/v1",
  "stove0-effect-target/v1"
]
```

### `/external_contract/protocol_schemas/generated:stove0-target/semantic_acceptance`

<!-- exact-contract-value: 69c1750b5af0c70f85cd8837f26d3095cd1eff97666f606198ef17766c2167a8 -->

```json
{
  "binding": "OperationContract.intent_semantics",
  "identity": [
    "id",
    "profile_sha256"
  ],
  "kind": "operation-contract",
  "request_response_relations": "required"
}
```

</details>
