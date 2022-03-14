Select 
    id as Customer_id,
    first_name,
    last_name
 FROM {{source ('jaffle_shop', 'customers')}}