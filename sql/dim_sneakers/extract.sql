with events as (
	select
		sneaker_id,
		sum(case when event_type = 'wear' then 1 else 0 end) as count_wears,
		sum(case when event_type = 'clean' then 1 else 0 end) as count_cleans,
		sum(case when event_type = 'walk' then 1 else 0 end) as count_walks
	from sneaker_events
	group by 1
)
select 
	s.id as sneaker_id,
	s.sneaker_name,
	s.color,
	s.created_at,
	case when coalesce(s.sold_at, s.trashed_at, s.given_at) is null then TRUE else FALSE end as is_owned,
	s.sold_at,
	s.trashed_at,
	s.given_at,
	m.manufacturer_name,
	m.collaborator_name,
	e.count_wears,
	e.count_cleans,
	e.count_walks,
	DATE('{{ ds }}') as updated_at
from sneakers s
left join manufacturers m on s.manufacturer_id = m.id
left join events e on s.id = e.sneaker_id