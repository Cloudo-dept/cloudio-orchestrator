"""What each kind of failure costs the requester, as data.

One row per ``FailureKind``, read by two consumers: the executor asks *may I retry this*, and the
escalator asks *do I open an incident, and what do I tell the requester when I close their ticket*.
Adding a kind — or moving a failure that is not a DAG failure onto this table later — is a row, not
a branch.
"""

from pydantic import BaseModel

from orchestrator.domain import FailureKind


class FailurePolicy(BaseModel):
    """The handling of one kind of failure."""

    retryable: bool  # False → terminal on the first failure, no backoff, no second engine run
    open_incident: bool  # True → an Incident to the responsible group before closing the ticket
    close_comment: str  # note the ticket is closed with; may reference {incident_id}


# The requester's ticket is always closed on a permanent failure — only the reason differs.
VALIDATION_COMMENT = "Your request was closed due to a validation error"

FAILURE_POLICIES: dict[FailureKind, FailurePolicy] = {
    # The request itself is wrong: a second run fails identically, and nobody needs paging.
    FailureKind.VALIDATION: FailurePolicy(
        retryable=False, open_incident=False, close_comment=VALIDATION_COMMENT
    ),
    # A precheck refused the request — same deal, same message to the requester.
    FailureKind.INFRA_PRECHECK: FailurePolicy(
        retryable=False, open_incident=False, close_comment=VALIDATION_COMMENT
    ),
    # Something broke while doing the work: retry first, then hand it to the responsible group.
    FailureKind.TASK: FailurePolicy(
        retryable=True,
        open_incident=True,
        close_comment="An error occurred. Incident {incident_id} was created",
    ),
}


def policy_for(kind: FailureKind) -> FailurePolicy:
    """The policy for ``kind``, defaulting to TASK's (incident + retries) for anything unmapped."""
    return FAILURE_POLICIES.get(kind, FAILURE_POLICIES[FailureKind.TASK])
