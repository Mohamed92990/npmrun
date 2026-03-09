-- Create a typed view on top of the raw CSV landing table.
-- Assumes raw table: public.karbon_timesheets_raw

create or replace view public.karbon_timesheets_typed as
select
  nullif("Date", '')::timestamptz as date_ts,
  nullif("Month", '')::int as month,
  nullif("Work", '') as work,
  nullif("Client", '') as client,
  nullif("Team_Member", '') as team_member,
  nullif("Role", '') as role,
  nullif("Task_Type", '') as task_type,
  nullif("Notes", '') as notes,
  nullif("Time_Minutes_", '')::numeric as time_minutes,
  nullif("Time_Hours_", '')::numeric as time_hours,
  nullif("Fee_Type", '') as fee_type,
  nullif("ClientID", '') as client_id,
  nullif("WorkID", '') as work_id
from public.karbon_timesheets_raw;
