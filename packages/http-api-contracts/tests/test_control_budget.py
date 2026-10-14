import pytest
from http_api_contracts.control import ControlBudgetExhausted, control_budget, control_timeout


@pytest.mark.parametrize("seconds", [0, -1, True, float("nan"), float("inf")])
def test_control_calls_cannot_opt_out_of_finite_allowance(seconds):
    with pytest.raises(ValueError, match="finite and positive"):
        control_timeout(seconds)


def test_nested_work_cannot_reset_the_enclosing_contact_budget(monkeypatch):
    now = 0.0
    monkeypatch.setattr("http_api_contracts.control.time.monotonic", lambda: now)
    with control_budget(2):
        now = 1
        with control_budget(30):
            assert control_timeout(5) == 1
            now = 3
            with pytest.raises(ControlBudgetExhausted):
                control_timeout()
        with pytest.raises(ControlBudgetExhausted):
            control_timeout()
    assert control_timeout(5) == 5
