"""Pure-Python tests for the fraud rules — no Kafka, no Postgres."""

from datetime import datetime, timezone

import pytest

# Adjust the import path to match your project layout.
from consumer.fraud_consumer import Transaction, evaluate, velocity_window


@pytest.fixture(autouse=True)
def _clear_velocity_state():
    velocity_window.clear()
    yield
    velocity_window.clear()


def make_txn(**overrides) -> Transaction:
    defaults = dict(
        transaction_id="t-1",
        account_id="ACC0000001",
        amount=100.0,
        merchant_category="Supermarkets",
        merchant_state="NSW",
        channel="EFTPOS",
        transaction_type="DEBIT",
        timestamp=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return Transaction(**defaults)


def test_normal_transaction_is_clean():
    reasons, score = evaluate(make_txn())
    assert reasons == []
    assert score == 0


def test_large_amount_triggers_both_amount_rules():
    reasons, score = evaluate(make_txn(amount=10_000))
    assert "Large amount (>$9k)" in reasons
    assert "Elevated amount (>$5k)" in reasons
    assert score == 70


def test_overseas_flagged():
    reasons, score = evaluate(make_txn(merchant_state="OVS"))
    assert "Overseas transaction" in reasons
    assert score == 30


def test_velocity_breach_on_fifth_txn():
    base = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    for i in range(4):
        reasons, _ = evaluate(make_txn(transaction_id=f"t-{i}", timestamp=base))
        assert "High velocity (5+ in 60s)" not in reasons
    reasons, _ = evaluate(make_txn(transaction_id="t-5", timestamp=base))
    assert "High velocity (5+ in 60s)" in reasons


def test_velocity_window_expires():
    from datetime import timedelta
    base = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    for i in range(5):
        evaluate(make_txn(transaction_id=f"t-{i}", timestamp=base))
    # 2 minutes later, the window should be empty again
    reasons, _ = evaluate(make_txn(transaction_id="t-late", timestamp=base + timedelta(minutes=2)))
    assert "High velocity (5+ in 60s)" not in reasons


def test_score_caps_at_100():
    # Large + elevated + overseas + online-odd-hour + velocity
    base = datetime(2025, 1, 1, 3, 0, tzinfo=timezone.utc)  # 3am
    for i in range(5):
        evaluate(make_txn(
            transaction_id=f"warmup-{i}",
            timestamp=base,
            amount=50, channel="EFTPOS", merchant_state="NSW",
        ))
    reasons, score = evaluate(make_txn(
        transaction_id="big",
        timestamp=base,
        amount=15_000,
        channel="ONLINE",
        merchant_state="OVS",
    ))
    assert score == 100
    assert len(reasons) >= 4
