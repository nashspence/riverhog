@acceptance @cli @mvp
Feature: arc-disc CLI
  The optical CLI is the no-argument physical-media and recovery backlog clearer.
  Targeted fetch commands remain available for explicit recovery detail flows.

  Rule: No-argument physical and recovery backlog
    Scenario: arc-disc reports a missing configured optical device
      Given statechart "arc_disc.guided" state "device_missing" is the accepted operator contract
      And the configured optical device path does not exist
      When the operator runs 'arc-disc'
      Then stderr includes operator copy "device_missing"
      And stderr mentions "Check the device path"
      And stderr does not mention "xorriso"

    Scenario: arc-disc reports optical device permission problems
      Given statechart "arc_disc.guided" state "device_permission_denied" is the accepted operator contract
      And the operator cannot read or write the configured optical device
      When the operator runs 'arc-disc'
      Then stderr includes operator copy "device_permission_denied"
      And stderr mentions "Fix device permissions"
      And stderr does not mention "PermissionError"

    Scenario: arc-disc reports device loss during physical work
      Given statechart "arc_disc.burn" state "device_lost_during_work" is the accepted operator contract
      And the optical device becomes unavailable while writing media
      When the operator runs 'arc-disc'
      Then stderr includes operator copy "device_lost_during_work"
      And stderr mentions "last safe checkpoint"
      And stderr does not mention "Input/output error"

    Scenario: arc-disc no-arg attention summary continues inside the guided flow
      Given statechart "arc_disc.guided" state "attention_summary" is the accepted operator contract
      And recovery data is ready for collection "docs"
      And ordinary blank-disc work is available
      When the operator runs 'arc-disc'
      Then stdout includes operator copy "arc_disc_attention"
      And stdout mentions "Press Enter"
      And stdout does not mention "Run arc-disc to clear this backlog in the safest order"
      When the operator confirms the next guided action
      Then statechart "arc_disc.guided" state "scan_backlog" is the accepted operator contract

    Scenario: arc-disc no-arg flow re-scans after distinct backlog work
      Given statechart "arc_disc.guided" state "recovery_ready" is the accepted operator contract
      And statechart "arc_disc.guided" state "burn_work_ready" is the accepted operator contract
      And recovery data is ready for collection "docs"
      And ordinary blank-disc work is available
      When the operator confirms the recovery action
      Then statechart "arc_disc.guided" state "scan_backlog" is the accepted operator contract
      When the recovery item is no longer waiting
      Then the guided flow chooses ordinary blank-disc work without another command

    Scenario: arc-disc reports API unreachability as accepted operator copy
      Given statechart "arc_disc.guided" state "api_unreachable" is the accepted operator contract
      And the Riverhog API is unreachable
      When the operator runs 'arc-disc'
      Then stdout includes operator copy "api_unreachable"
      And the operator decision matches the accepted state
      And stdout mentions "Riverhog cannot reach the API"
      And stdout mentions "local configuration"
      And stdout does not mention "httpx"

    @contract_gap @issue_208
    Scenario: arc-disc resumes unfinished local disc work before choosing new work
      Given statechart "arc_disc.guided" state "unfinished_local_disc" is the accepted operator contract
      And an unlabeled verified disc is waiting for label confirmation
      And ordinary blank-disc work is available
      When the operator runs 'arc-disc'
      Then the command exits with code 0
      And stdout includes operator copy "disc_item_unfinished_local_copy"
      And stdout mentions "Finish labeling"
      And stdout mentions "label"
      And stdout mentions "storage location"
      And stdout does not mention "candidate"

    @issue_208
    Scenario: arc-disc handles ready recovery before ordinary blank-disc work
      Given statechart "arc_disc.guided" state "recovery_ready" is the accepted operator contract
      And recovery data is ready for collection "docs"
      And ordinary blank-disc work is available
      When the operator runs 'arc-disc'
      Then the command exits with code 0
      And stdout includes operator copy "disc_item_recovery_ready"
      And stdout mentions "Recovery is ready"
      And stdout mentions "docs"
      And stdout mentions "replacement disc"
      And stdout does not mention "image_rebuild"

    @contract_gap @issue_208
    Scenario: arc-disc asks for recovery approval before ordinary blank-disc work
      Given statechart "arc_disc.guided" state "recovery_approval_required" is the accepted operator contract
      And recovery for collection "docs" needs approval
      And ordinary blank-disc work is available
      When the operator runs 'arc-disc'
      Then the command exits with code 0
      And stdout includes operator copy "disc_item_recovery_approval_required"
      And stdout mentions "Recovery needs approval"
      And stdout mentions "Estimated cost"
      And stdout mentions "docs"
      And stdout does not mention "pending_approval"

    @issue_208
    Scenario: arc-disc guides disc restore that needs media
      Given statechart "arc_disc.guided" state "hot_recovery_needs_media" is the accepted operator contract
      And pinned files need disc restore
      When the operator runs 'arc-disc'
      Then the command exits with code 0
      And stdout includes operator copy "disc_item_hot_recovery_needs_media"
      And stdout mentions "Disc restore needs a disc"
      And stdout mentions "Insert the requested disc"
      And stdout mentions target "docs/tax/2022/invoice-123.pdf"
      And stdout does not mention "fetch manifest"

    @contract_gap @issue_208
    Scenario: arc-disc clears ordinary blank-disc work only after label confirmation
      Given statechart "arc_disc.guided" state "burn_work_ready" is the accepted operator contract
      And statechart "arc_disc.burn" state "backlog_cleared" is the accepted operator contract
      And ordinary blank-disc work is available
      And the operator confirms labeled disc at storage location "vault-a/shelf-01"
      When the operator runs 'arc-disc'
      Then the command exits with code 0
      And stdout includes operator copy "burn_backlog_cleared"
      And stdout mentions "Disc work complete"
      And stdout mentions "label"
      And stdout mentions "storage location"
      And the collection is fully protected

    @contract_gap @issue_208
    Scenario: arc-disc does not count an unlabeled disc as protected
      Given statechart "arc_disc.burn" state "label_checkpoint" is the accepted operator contract
      And ordinary blank-disc work is available
      When the operator runs 'arc-disc' without label confirmation
      Then the command exits non-zero
      And stderr includes operator copy "burn_label_checkpoint"
      And stderr mentions "label"
      And the collection is not fully protected

  Rule: Targeted fetch detail remains available
  Scenario: arc-disc fetch names the exact same-image disc after rejected bytes
    Given statechart "arc_disc.fetch" state "retry_other_disc" is the accepted operator contract
    And fetch "fx-1" needs copy label "20260420T040003Z-1"
    And same-image copy label "20260420T040003Z-2" remains untried
    When the server rejects recovered bytes from copy label "20260420T040003Z-1"
    Then stderr includes operator copy "hot_recovery_retry_other_disc"
    And stderr mentions "20260420T040003Z-2"
    And stderr does not mention "try another registered copy or recovered media"
    And stderr does not mention "20260420T040004Z-1"

  Scenario: arc-disc disc restore names the exact same-image disc after failed media
    Given statechart "arc_disc.hot_recovery" state "retry_other_disc" is the accepted operator contract
    And disc restore needs copy label "20260420T040003Z-1"
    And same-image copy label "20260420T040003Z-2" remains untried
    When copy label "20260420T040003Z-1" cannot restore the requested files
    Then stderr includes operator copy "hot_recovery_retry_other_disc"
    And stderr mentions "20260420T040003Z-2"
    And stderr does not mention "try another registered disc or recovered media"
    And stderr does not mention "20260420T040004Z-1"

  Scenario: arc-disc failed media routes to recovery when same-image copies are exhausted
    Given statechart "arc_disc.fetch" state "recovery_workflow_needed" is the accepted operator contract
    And all registered same-image disc labels for fetch "fx-1" have failed
    When the operator runs 'arc-disc fetch "fx-1"'
    Then stderr includes operator copy "hot_recovery_registered_copies_exhausted"
    And stderr mentions "recovery workflow"
    And stderr does not mention "try another registered copy"

  Rule: API unreachability preflight
    @issue_288
    Scenario: arc-disc burn reports API unreachability as accepted operator copy
      Given statechart "arc_disc.burn" state "api_unreachable" is the accepted operator contract
      And the Riverhog API is unreachable
      When the operator runs arc-disc burn
      Then stdout includes operator copy "api_unreachable"
      And the operator decision matches the accepted state
      And stdout does not mention "httpx"

    @issue_288
    Scenario: arc-disc recover reports API unreachability as accepted operator copy
      Given statechart "arc_disc.recovery" state "api_unreachable" is the accepted operator contract
      And the Riverhog API is unreachable
      When the operator runs arc-disc recover "rs-20260420T040001Z-rebuild-1"
      Then stdout includes operator copy "api_unreachable"
      And the operator decision matches the accepted state
      And stdout does not mention "httpx"

    @issue_288
    Scenario: arc-disc fetch reports API unreachability as accepted operator copy
      Given statechart "arc_disc.fetch" state "api_unreachable" is the accepted operator contract
      And the Riverhog API is unreachable
      When the operator runs arc-disc fetch "fx-1"
      Then stdout includes operator copy "api_unreachable"
      And the operator decision matches the accepted state
      And stdout does not mention "httpx"

    @issue_288
    Scenario: arc-disc restore reports API unreachability as accepted operator copy
      Given statechart "arc_disc.hot_recovery" state "api_unreachable" is the accepted operator contract
      And the Riverhog API is unreachable
      When the operator runs 'arc-disc restore'
      Then stdout includes operator copy "api_unreachable"
      And the operator decision matches the accepted state
      And stdout does not mention "httpx"

  @ci_opt_in @requires_optical_disc_drive @requires_human_operator @issue_186 @issue_187
  Scenario: arc-disc fetch completes a recoverable fetch
    Given split archived target "docs/tax/2022/invoice-123.pdf" is pinned with fetch "fx-1"
    And fetch "fx-1" has a stable manifest
    And a configured optical reader can recover every required entry
    When the operator runs arc-disc fetch "fx-1" with JSON output
    Then the command exits with code 0
    And stdout is valid JSON
    And stdout reports fetch state "done"
    And stderr mentions copy id "20260420T040003Z-1"
    And stderr mentions copy id "20260420T040004Z-1"
    And target for fetch "fx-1" is hot

  @ci_opt_in @requires_optical_disc_drive @requires_human_operator @issue_186 @issue_187
  Scenario: arc-disc fetch reports precise progress while streaming uploads
    Given split archived target "docs/tax/2022/invoice-123.pdf" is pinned with fetch "fx-1"
    And fetch "fx-1" has a stable manifest
    And a configured optical reader can recover every required entry
    When the operator runs arc-disc fetch "fx-1" with JSON output
    Then the command exits with code 0
    And stderr mentions "current file"
    And stderr mentions "manifest"
    And stderr mentions "%"
    And stderr mentions "/s"

  @ci_opt_in @requires_optical_disc_drive @requires_human_operator @issue_186 @issue_187
  Scenario: arc-disc fetch fails if optical recovery fails
    Given split archived target "docs/tax/2022/invoice-123.pdf" is pinned with fetch "fx-1"
    And fetch "fx-1" has a stable manifest
    And a configured optical reader can recover every required entry
    And the configured optical reader cannot recover one required entry
    When the operator runs arc-disc fetch "fx-1"
    Then the command exits non-zero
    And fetch "fx-1" is not "done"

  @ci_opt_in @requires_optical_disc_drive @requires_human_operator @issue_186 @issue_187
  Scenario: arc-disc fetch resumes split recovery across repeated runs via server-side upload state
    Given split archived target "docs/tax/2022/invoice-123.pdf" is pinned with fetch "fx-1"
    And fetch "fx-1" has a stable manifest
    And a configured optical reader can recover every required entry
    And the configured optical reader cannot recover copy id "20260420T040004Z-1"
    When the operator runs arc-disc fetch "fx-1"
    Then the command exits non-zero
    And fetch "fx-1" is not "done"
    When the configured optical reader cannot recover copy id "20260420T040003Z-1"
    And the operator runs arc-disc fetch "fx-1" with JSON output
    Then the command exits with code 0
    And stdout is valid JSON
    And stdout reports fetch state "done"
    And stderr does not mention copy id "20260420T040003Z-1"
    And stderr mentions copy id "20260420T040004Z-1"
    And target for fetch "fx-1" is hot

  @ci_opt_in @requires_optical_disc_drive @requires_human_operator @issue_186 @issue_187
  Scenario: arc-disc fetch fails if the server rejects incorrect recovered bytes
    Given split archived target "docs/tax/2022/invoice-123.pdf" with same-image copy label "20260420T040003Z-2" is pinned with fetch "fx-1"
    And fetch "fx-1" has a stable manifest
    And a configured optical reader can recover every required entry
    And the configured optical reader returns bytes the server rejects for one required entry
    When the operator runs arc-disc fetch "fx-1"
    Then the command exits non-zero
    And fetch "fx-1" is not "done"
    And stderr mentions "reset byte-complete upload"
    And stderr mentions copy id "20260420T040003Z-2"
    And stderr does not mention "try another registered copy or recovered media"
    When the client gets "/v1/fetches/fx-1/manifest"
    Then the response status is 200
    And fetch manifest entry "e1" upload state is "pending"
    And fetch manifest entry "e1" uploaded bytes is 0
    When a configured optical reader can recover every required entry
    And the operator runs arc-disc fetch "fx-1" with JSON output
    Then the command exits with code 0
    And stdout is valid JSON
    And stdout reports fetch state "done"
    And target for fetch "fx-1" is hot
