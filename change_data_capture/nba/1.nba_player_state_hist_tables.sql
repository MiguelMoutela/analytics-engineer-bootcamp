--drop table miguelmoutela.nba_player_tracked;
create table miguelmoutela.nba_player_tracked (
    player_name varchar,
    is_active boolean,
    latest_season integer
);

--drop stream miguelmoutela.nba_player_flow;
create stream miguelmoutela.nba_player_flow on table miguelmoutela.nba_player_tracked;

--drop table miguelmoutela.nba_player_flow_materialized;
create table miguelmoutela.nba_player_flow_materialized (
    player_name varchar,
    is_active boolean,
    action varchar,
    is_update boolean,
    row_id varchar,
    season integer,
    last_updated_ts timestamp
);
