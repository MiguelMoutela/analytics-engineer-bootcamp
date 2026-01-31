create or replace procedure miguelmoutela.proc_nba_player_cdc_flow(p_season int)
returns string
language sql
as

declare
    v_season integer;
    result string;
begin
    v_season := p_season;

    merge into miguelmoutela.nba_player_tracked tgt using (
        with before as (

            select *
            from miguelmoutela.nba_player_tracked
            where latest_season = :v_season - 1
            and is_active = true
        ), after as (
            select *
            from bootcamp.nba_player_seasons
            where season = :v_season
        )
        select 
            coalesce(
                after.player_name, 
                before.player_name
            ) as player_name,
            after.player_name is not null as is_active,
            coalesce(
                after.season, 
                before.latest_season
            ) as season
        from before 
        full outer join after on (
            before.player_name = after.player_name
        )
    ) src 
        on src.player_name = tgt.player_name
    when not matched then 
        insert (
            player_name, 
            is_active, 
            latest_season 
        ) 
        values ( 
            src.player_name, 
            true, 
            src.season
        )
    when matched then 
        update set tgt.latest_season = :v_season, 
                   tgt.is_active = src.is_active
    ;
    
    insert into miguelmoutela.nba_player_flow_materialized
    select
        player_name,
        is_active,
        metadata$action,
        metadata$isupdate,
        metadata$row_id,
        latest_season as season,
        current_timestamp()
    from miguelmoutela.nba_player_flow
    where metadata$action = 'INSERT'
    ;

    result := 'Done processing ' || v_season;
    return result;
end
;
