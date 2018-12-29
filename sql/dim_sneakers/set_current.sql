UPDATE dim_sneakers 
SET is_current = FALSE
WHERE updated_at < date('{{ ds }}');
