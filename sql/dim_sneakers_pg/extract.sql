with events as (
	select
		sneaker_id,
		sum(case when event_type = 'wear' then 1 else 0 end) as count_wears,
		sum(case when event_type = 'clean' then 1 else 0 end) as count_cleans,
		sum(case when event_type = 'walk' then 1 else 0 end) as count_walks
	from sneaker_events
	where event_time <= TO_TIMESTAMP({{ ds }}) with TIME zone 'UTC'
	group by 1
)
insert into dim_sneakers_staging
select 
	s.id as sneaker_id,
	s.sneaker_name,
	s.color,
	s.created_at,
	case when (s.sold_at is null and s.trashed_at is null and s.given_at is null) then TRUE else FALSE end as is_owned,
	s.sold_at,
	s.trashed_at,
	s.given_at,
	m.manufacturer_name,
	m.collaborator_name,
	e.count_wears,
	e.count_cleans,
	e.count_walks
from sneakers s
where s.created_at <= TO_TIMESTAMP({{ ds }}) with TIME zone 'UTC'
left join manufacturers m on s.manufacturer_id = m.id
left join events e on s.id = e.sneaker_id
