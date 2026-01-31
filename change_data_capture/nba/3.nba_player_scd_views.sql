
create or replace view miguelmoutela.nba_player_tracked_scd1 as
select
    player_name,
    is_active,
    season as valid_from_season,
    lead(valid_from_season)
        over (
            partition by player_name 
            order by valid_from_season 
    ) as valid_to_season,
    last_updated_ts
from miguelmoutela.nba_player_flow_materialized
;


create or replace view miguelmoutela.nba_player_tracked_scd2 as
with active_status_changes as (
    select
        player_name,
        is_active,
        valid_from_season,
        valid_to_season,
        last_updated_ts,
        lag(is_active) over (
            partition by player_name
            order by valid_from_season
        ) as prev_is_active
    from miguelmoutela.nba_player_tracked_scd1
)
, active_status_change_groups as (
    select 
        player_name,
        is_active,
        valid_from_season,
        valid_to_season,
        last_updated_ts,
        sum(case 
                when prev_is_active is null 
                or prev_is_active <> is_active
                    then 1 
                    else 0 
            end
        ) over (
            partition by player_name
            order by valid_from_season
        ) as status_change_group
    from active_status_changes
)
, active_status_changes_ordered as (
    select
        player_name,
        is_active,
        valid_from_season,
        ifnull(valid_to_season, 9999) as valid_to_season,
        last_updated_ts,
        row_number() over (
            partition by player_name, 
                         status_change_group 
            order by valid_from_season asc
        ) as rn_asc_valid_from_season,
        row_number() over (
            partition by player_name, 
                         status_change_group 
            order by valid_to_season desc nulls first
        ) as rn_desc_valid_to_season,
        row_number() over (
            partition by player_name, 
                         status_change_group 
            order by last_updated_ts desc
        ) as rn_desc_last_updated_ts,
        status_change_group
    from active_status_change_groups
)
, active_status_changes_rn as (
    select 
        player_name,
        is_active,
        case 
            when rn_asc_valid_from_season = 1 
                then valid_from_season 
                else 0 
        end as r1_valid_from_season,
        case 
            when rn_desc_valid_to_season = 1 
                 then valid_to_season 
                 else 0 
        end as r1_valid_to_season,
        case 
            when rn_desc_last_updated_ts = 1 
            then last_updated_ts 
            else NULL 
        end as r1_last_updated_ts,
        status_change_group
    from active_status_changes_ordered
)
, active_status_changes_summary as (
    select 
        player_name, 
        is_active, 
        sum(r1_valid_from_season) over (
            partition by player_name, 
                         status_change_group
        ) as valid_from_season, 
        sum(r1_valid_to_season) over (
            partition by player_name, 
                         status_change_group
        ) as valid_to_season,
        max(r1_last_updated_ts) over (
            partition by player_name, 
                         status_change_group
        ) as last_updated_ts
    from active_status_changes_rn
)
select distinct
    player_name, 
    is_active, 
    valid_from_season, 
    case 
        when valid_to_season = 9999 
             then NULL 
             else valid_to_season 
    end as valid_to_season,
    last_updated_ts
    from active_status_changes_summary
;


