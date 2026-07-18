import os
from typing import Any
from unittest.mock import MagicMock, patch

from jobs.ufa_api import flow

Executed = list[tuple[str, dict[str, Any] | None]]


def _mock_engine(db_ids: set[str], picked_ids: set[str]) -> tuple[MagicMock, Executed]:
    """Build a mock engine whose connection answers the two SELECTs and records executes.

    Returns the engine and a list of ``(sql, params)`` tuples for every ``execute`` call,
    so tests can assert which DELETE statements ran.
    """
    executed: Executed = []

    def execute(statement: object, params: dict[str, Any] | None = None) -> MagicMock:
        sql = str(statement)
        executed.append((sql, params))
        result = MagicMock()
        if "FROM games WHERE season" in sql:
            result.fetchall.return_value = [(gid,) for gid in db_ids]
        elif "FROM picks WHERE game_id" in sql:
            result.fetchall.return_value = [(gid,) for gid in picked_ids]
        else:
            result.fetchall.return_value = []
        return result

    connection = MagicMock()
    connection.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    return engine, executed


def _deletes(executed: Executed, table: str) -> list[dict[str, Any]]:
    """Return the params of every ``DELETE FROM <table>`` statement that ran."""
    result = []
    for sql, params in executed:
        if f"DELETE FROM {table}" in sql:
            assert params is not None
            result.append(params)
    return result


def _run(db_ids: set[str], api_ids: list[str], picked_ids: set[str]) -> tuple[Executed, MagicMock]:
    """Run delete_orphaned_games against a mock DB, returning executes and the discord mock."""
    engine, executed = _mock_engine(db_ids, picked_ids)
    with (
        patch.dict(os.environ, {"DISCORD_ALERT_URL": "https://discord.test/webhook"}),
        patch.object(flow, "_get_db_engine", return_value=engine),
        patch.object(flow, "get_run_logger", return_value=MagicMock()),
        patch("jobs.ufa_api.flow.razator_utils.discord_message") as discord,
    ):
        flow.delete_orphaned_games.fn(2026, api_ids)
    return executed, discord


def test_orphan_without_picks_is_deleted() -> None:
    """A game missing from the API with no picks is deleted; no alert is sent."""
    executed, discord = _run(db_ids={"g1", "g2"}, api_ids=["g1"], picked_ids=set())

    game_deletes = _deletes(executed, "games")
    assert len(game_deletes) == 1
    assert set(game_deletes[0]["ids"]) == {"g2"}
    discord.assert_not_called()


def test_orphan_with_picks_is_preserved_and_alerts() -> None:
    """A game missing from the API that has picks is NOT deleted and triggers a Discord alert."""
    executed, discord = _run(db_ids={"g1", "g2"}, api_ids=["g1"], picked_ids={"g2"})

    assert _deletes(executed, "games") == []  # nothing deleted
    assert _deletes(executed, "picks") == []  # picks are never touched
    discord.assert_called_once()
    _url, message = discord.call_args.args
    assert "g2" in message


def test_mixed_orphans_delete_empty_only() -> None:
    """When some orphans have picks and some don't, only the empty ones are deleted."""
    executed, discord = _run(db_ids={"g1", "g2", "g3"}, api_ids=["g1"], picked_ids={"g2"})

    game_deletes = _deletes(executed, "games")
    assert len(game_deletes) == 1
    assert set(game_deletes[0]["ids"]) == {"g3"}  # g3 empty deleted, g2 protected
    discord.assert_called_once()
    _url, message = discord.call_args.args
    assert "g2" in message
    assert "g3" not in message


def test_no_orphans_does_nothing() -> None:
    """When every DB game is present in the API, nothing is deleted and no alert is sent."""
    executed, discord = _run(db_ids={"g1", "g2"}, api_ids=["g1", "g2"], picked_ids=set())

    assert _deletes(executed, "games") == []
    discord.assert_not_called()
