create table student_api.users(
    user_id integer, 
    timezone text
);


insert into student_api.users (user_id, timezone)
values (-999, 'mock_timezone');


create table student_api.timezone_audit_tracking(
    user_id integer,
    operation_type text,
    old_timezone,
    new_timezone,
    update_time timestamp
);


create or replace function student_api.fn_audit_timezone_update()
returns trigger as $$
begin
    if (old.timezone is distinct from new.timezone)
    then
        insert into student_api.timezone_audit_tracking (
            user_id,
            operation_name,
            old_timezone,
            new_timezone,
            update_time
        )
        values (
            new.user_id,
            TG_OP,
            old.timezeone,
            new.timezone,
            current_timestamp
        );
    end if;
    return new;
end;
$$ language plpgsql;


create trigger trg_audit_timeozone_update
after update on student_api.users
for each row
when (old.timezone is distinct from new.timezone)
execute function student_api.fn_audit_timezone_update();