"""骨架冒烟测试。"""

from __future__ import annotations

from app.models import TERMINAL_STATES, JobState


def test_state_enum():
    assert JobState.PENDING.value == "PENDING"
    assert JobState.SUCCEEDED in TERMINAL_STATES
