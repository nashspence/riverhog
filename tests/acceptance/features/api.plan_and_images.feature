@acceptance @api @mvp @xfail_contract
Feature: Plan and images API
  The API reports the current best image plan, exposes image summaries, and registers physical copies.

  Background:
    Given an archive with planner fixtures
    And the planner has at least one candidate image

  Scenario: Read the current plan
    When the client gets "/v1/plan"
    Then the response status is 200
    And the response contains "ready", "target_bytes", "min_fill_bytes", "images", and "unplanned_bytes"
    And each plan image contains "candidate_id", "bytes", "fill", "files", "collections", and "iso_ready"
    And plan images do not contain field "volume_id"
    And each image fill equals image bytes divided by target bytes
    And images are returned in best-first order

  Scenario: Finalized image lookup requires an existing finalized id
    Given candidate "img_2026-04-20_01" exists
    When the client gets "/v1/images/20260420T040001Z"
    Then the response status is 404
    And the error code is "not_found"

  Scenario: Explicitly finalizing a candidate creates a finalized image and removes it from the plan
    Given candidate "img_2026-04-20_01" exists
    When the client posts to "/v1/plan/candidates/img_2026-04-20_01/finalize"
    Then the response status is 200
    And the response contains image id "20260420T040001Z"
    And the response does not contain field "volume_id"
    When the client gets "/v1/plan"
    Then the response status is 200
    And the response images do not contain image id "img_2026-04-20_01"
    When the client gets "/v1/images/20260420T040001Z"
    Then the response status is 200
    And the response contains image id "20260420T040001Z"
    And the response does not contain field "volume_id"

  Scenario: List finalized images separately from the provisional plan
    Given an archive with split planner fixtures
    And candidate "img_2026-04-20_01" is finalized
    And candidate "img_2026-04-20_03" is finalized
    And candidate "img_2026-04-20_04" is finalized
    When the client gets "/v1/images"
    Then the response status is 200
    And the response contains "page", "per_page", "total", "pages", "sort", "order", and "images"
    And each finalized image contains "id", "filename", "finalized_at", "bytes", "fill", "files", "collections", "collection_ids", "iso_ready", and "copy_count"
    And finalized images are returned newest-first
    And each finalized image is iso-ready

  Scenario: Finalized image listing honors pagination
    Given an archive with split planner fixtures
    And candidate "img_2026-04-20_01" is finalized
    And candidate "img_2026-04-20_03" is finalized
    And candidate "img_2026-04-20_04" is finalized
    When the client gets "/v1/images?page=1&per_page=2"
    Then the response status is 200
    And the response pagination is page 1 with per_page 2 and total 3 and pages 2
    And the response contains 2 finalized images
    And the response finalized images include "20260420T040004Z" and "20260420T040003Z"

  Scenario: Finalized image listing can filter by copy presence
    Given an archive with split planner fixtures
    And candidate "img_2026-04-20_01" is finalized
    And candidate "img_2026-04-20_03" is finalized
    And copy "BR-021-A" already exists
    When the client gets "/v1/images?has_copies=true"
    Then the response status is 200
    And the response finalized images contain only "20260420T040001Z"
    And each finalized image has copy_count greater than 0

  Scenario: Finalized image listing can filter by filename query and contained collection
    Given candidate "img_2026-04-20_01" is finalized
    And fixture finalized image "20260420T040002Z" exists for collection "photos-2024"
    When the client gets "/v1/images?q=040002Z.iso&collection=photos-2024"
    Then the response status is 200
    And the response finalized images contain only "20260420T040002Z"

  Scenario: Repeating candidate finalization reuses the same finalized image id
    Given candidate "img_2026-04-20_01" exists
    When the client posts to "/v1/plan/candidates/img_2026-04-20_01/finalize"
    And the client posts to "/v1/plan/candidates/img_2026-04-20_01/finalize" again
    Then the response status is 200 both times
    And both responses contain the same value for field "id"

  Scenario: Downloading an ISO for a finalized id that does not exist yet fails
    Given candidate "img_2026-04-20_01" exists
    When the client gets "/v1/images/20260420T040001Z/iso"
    Then the response status is 404
    And the error code is "not_found"

  Scenario: Download an ISO for a finalized image
    Given candidate "img_2026-04-20_01" is finalized
    When the client gets "/v1/images/20260420T040001Z/iso"
    Then the response status is 200
    And the response body is binary ISO content

  Rule: Downloaded ISOs match the published disc contracts
    Scenario: A ready image uses the canonical disc layout and metadata contracts
      Given candidate "img_2026-04-20_01" is finalized
      When the client downloads and inspects ISO for image "20260420T040001Z"
      Then the response status is 200
      And the downloaded ISO passes xorriso verification
      And the extracted ISO root matches the disc layout contract
      And the decrypted disc manifest matches the disc manifest contract
      And every referenced collection manifest matches the collection hash manifest contract
      And every referenced file sidecar matches the file sidecar contract
      And the current ISO README documents split-file recovery
      And the current ISO payload for "docs/tax/2022/invoice-123.pdf" decrypts to the original plaintext

    Scenario: Split image parts are listed per disc and reconstruct the logical file
      Given an archive with split planner fixtures
      And candidate "img_2026-04-20_03" is finalized
      And candidate "img_2026-04-20_04" is finalized
      When the client downloads and inspects ISO for image "20260420T040003Z"
      Then the response status is 200
      And the downloaded ISO passes xorriso verification
      And the extracted ISO root matches the disc layout contract
      And the decrypted disc manifest matches the disc manifest contract
      And every referenced collection manifest matches the collection hash manifest contract
      And every referenced file sidecar matches the file sidecar contract
      And the current ISO lists split file "/tax/2022/invoice-123.pdf" part 1 of 2
      And the current split payload for "/tax/2022/invoice-123.pdf" is recorded
      When the client downloads and inspects ISO for image "20260420T040004Z"
      Then the response status is 200
      And the downloaded ISO passes xorriso verification
      And the extracted ISO root matches the disc layout contract
      And the decrypted disc manifest matches the disc manifest contract
      And every referenced collection manifest matches the collection hash manifest contract
      And every referenced file sidecar matches the file sidecar contract
      And the current ISO lists split file "/tax/2022/invoice-123.pdf" part 2 of 2
      And the current split payload for "/tax/2022/invoice-123.pdf" is recorded
      And the recorded split payloads for "docs/tax/2022/invoice-123.pdf" reconstruct the original plaintext

  Rule: Registering a copy increases archived coverage
    Background:
      Given candidate "img_2026-04-20_01" covers bytes from collection "docs"
      And candidate "img_2026-04-20_01" is finalized

    Scenario: Register a physical copy
      When the client posts to "/v1/images/20260420T040001Z/copies" with id "BR-021-A" and location "Shelf B1"
      Then the response status is 200
      And the response contains copy id "BR-021-A"
      And the response copy contains "volume_id", "location", and "created_at"
      And collection "docs" archived_bytes increases
      And collection "docs" pending_bytes decreases

    Scenario: Reusing a copy id for the same finalized image fails
      Given copy "BR-021-A" already exists
      When the client posts to "/v1/images/20260420T040001Z/copies" with id "BR-021-A" and location "Shelf B2"
      Then the response status is 409
      And the error code is "conflict"
