import datetime as dt
import os

import pandas as pd
from audl.stats.endpoints.seasonschedule import SeasonSchedule
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from sqlalchemy import Engine, create_engine, text

from hooks import discord_failure_hook

RENAME_COLS = {
    "gameID": "id",
    "awayTeamID": "away_team_id",
    "homeTeamID": "home_team_id",
    "awayScore": "away_score",
    "homeScore": "home_score",
    "status": "status",
    "week": "week",
    "streamingURL": "streaming_url",
    "hasRosterReport": "has_roster_report",
    "startTimestamp": "start_timestamp",
    "startTimezone": "start_timezone",
    "startTimeTBD": "start_time_tbd",
    "season": "season",
}


def _get_db_engine() -> Engine:
    return create_engine(
        f"postgresql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@"
        f"{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )


def _upsert_rows(df: pd.DataFrame, table_name: str, id_col: str | list[str]) -> None:
    logger = get_run_logger()
    if isinstance(id_col, str):
        id_col = [id_col]
    engine = _get_db_engine()
    try:
        with engine.connect() as connection:
            for _, row in df.iterrows():
                insert_stmt = text(f"""
                    INSERT INTO {table_name} ({", ".join(df.columns)})
                    VALUES ({", ".join([":" + col for col in df.columns])})
                    ON CONFLICT ({", ".join(id_col)}) DO UPDATE
                    SET {", ".join([f"{col} = :{col}" for col in df.columns if col not in id_col])};
                """)
                connection.execute(insert_stmt, row.to_dict())
            connection.commit()
        logger.info(f"DataFrame upserted to table {table_name}.")
    finally:
        engine.dispose()


@task(cache_policy=NO_CACHE)
def fetch_season_schedule(season: int) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Fetching season schedule for {season}")
    df = SeasonSchedule(season).get_schedule()
    df["season"] = str(season)
    df = df.rename(columns=RENAME_COLS)
    logger.info(f"Fetched {len(df)} games")
    return df


@task(cache_policy=NO_CACHE)
def sync_teams(df: pd.DataFrame) -> None:
    teams_df = df[["home_team_id", "homeTeamCity", "homeTeamName"]].drop_duplicates()
    teams_df = teams_df.rename(
        columns={
            "home_team_id": "id",
            "homeTeamCity": "team_city",
            "homeTeamName": "team_name",
        }
    )
    _upsert_rows(teams_df, "teams", "id")


@task(cache_policy=NO_CACHE)
def delete_orphaned_games(season: int, api_ids: list[str]) -> None:
    logger = get_run_logger()
    engine = _get_db_engine()
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT id FROM games WHERE season = :season"), {"season": str(season)}
            )
            db_ids = {row[0] for row in result.fetchall()}
            orphaned_ids = db_ids - set(api_ids)
            if orphaned_ids:
                logger.info(
                    f"Deleting {len(orphaned_ids)} orphaned games for season {season}:"
                    f" {orphaned_ids}"
                )
                connection.execute(
                    text("DELETE FROM picks WHERE game_id IN :ids"), {"ids": tuple(orphaned_ids)}
                )
                connection.execute(
                    text("DELETE FROM games WHERE id IN :ids"), {"ids": tuple(orphaned_ids)}
                )
                connection.commit()
                logger.info("Deleted orphaned games and associated picks.")
            else:
                logger.info("No orphaned games to delete.")
    finally:
        engine.dispose()


@task(cache_policy=NO_CACHE)
def upsert_games(df: pd.DataFrame) -> None:
    df = df[df.week != ""]
    df = df[df.id != "2025-08-23-allstar-game"]
    df["week"] = df.week.str.replace("week-", "").astype(int)
    df["start_timestamp"] = pd.to_datetime(df.start_timestamp, utc=True)
    _upsert_rows(df[list(RENAME_COLS.values())], "games", "id")


@flow(name="ufa-api", on_failure=[discord_failure_hook])
def update_ufa_data(season: int = dt.datetime.now().year, update_teams_only: bool = False) -> None:
    logger = get_run_logger()
    logger.info(f"Updating UFA data for season {season}")
    df = fetch_season_schedule(season)
    if update_teams_only:
        sync_teams(df)
        return
    delete_orphaned_games(season, df["id"].tolist())
    upsert_games(df)


if __name__ == "__main__":
    update_ufa_data()
