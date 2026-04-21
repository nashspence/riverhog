@acceptance @cli @mvp
Feature: arc-disc CLI
  The optical CLI fulfills a fetch from disc media and completes it through the API.

  Background:
    Given split archived target "docs:/tax/2022/invoice-123.pdf" is pinned with fetch "fx-1"
    And fetch "fx-1" has a stable manifest
    And a fake optical reader fixture can recover every required encrypted entry
    And a fake crypto fixture can decrypt every required entry

  Scenario: arc-disc fetch completes a recoverable fetch
    When the operator runs 'arc-disc fetch fx-1 --state-dir /tmp/arc/fx-1 --device /dev/fake-sr0 --json'
    Then the command exits with code 0
    And stdout is valid JSON
    And stdout reports fetch state "done"
    And stderr mentions copy id "copy-docs-split-1"
    And stderr mentions copy id "copy-docs-split-2"
    And target for fetch "fx-1" is hot

  Scenario: arc-disc fetch fails if optical recovery fails
    Given the optical reader fixture fails for one required entry
    When the operator runs 'arc-disc fetch fx-1 --state-dir /tmp/arc/fx-1 --device /dev/fake-sr0'
    Then the command exits non-zero
    And fetch "fx-1" is not "done"

  Scenario: arc-disc fetch resumes split recovery from an existing state directory
    Given the optical reader fixture fails for copy id "copy-docs-split-2"
    When the operator runs 'arc-disc fetch fx-1 --state-dir /tmp/arc/fx-1 --device /dev/fake-sr0'
    Then the command exits non-zero
    And the recovery state directory contains staged part 0 for entry "e1"
    And fetch "fx-1" is not "done"
    When the optical reader fixture fails for copy id "copy-docs-split-1"
    And the operator runs 'arc-disc fetch fx-1 --state-dir /tmp/arc/fx-1 --device /dev/fake-sr0 --json'
    Then the command exits with code 0
    And stdout is valid JSON
    And stdout reports fetch state "done"
    And stderr does not mention copy id "copy-docs-split-1"
    And stderr mentions copy id "copy-docs-split-2"
    And target for fetch "fx-1" is hot

  Scenario: arc-disc fetch fails if decrypted bytes do not match the expected hash
    Given the crypto fixture returns incorrect plaintext for one required entry
    When the operator runs 'arc-disc fetch fx-1 --state-dir /tmp/arc/fx-1 --device /dev/fake-sr0'
    Then the command exits non-zero
    And fetch "fx-1" is not "done"
