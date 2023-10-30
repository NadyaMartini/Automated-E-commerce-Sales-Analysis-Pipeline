    delete
    from mart.f_customer_retention
    where period_id=(select week_of_year from mart.d_calendar where date_actual = '{{ds}}'::DATE);

    with customers as
         (select *
          from mart.f_sales
                   join mart.d_calendar on f_sales.date_id = d_calendar.date_id
          where week_of_year = DATE_PART('week', '{{ds}}'::DATE)),
     new_customers as
         (select customer_id
          from customers
          where status = 'shipped'
          group by customer_id
          having count(*) = 1),
     returning_customers as
         (select customer_id
          from customers
          where status = 'shipped'
          group by customer_id
          having count(*) > 1),
     refunded_customers as
         (select customer_id
          from customers
          where status = 'refunded'
          group by customer_id)
    insert into mart.f_customer_retention (new_customers_count, new_customers_revenue, returning_customers_count, returning_customers_revenue, refunded_customer_count, customers_refunded, period_id, period_name, item_id, city_id)
    select COALESCE(new_customers.customers, 0)                                                                    as new_customers_count,
       COALESCE(new_customers.revenue, 0)                                                                      as new_customers_revenue,
       COALESCE(returning_customers.customers, 0)                                                              as returning_customers_count,
       COALESCE(returning_customers.revenue, 0)                                                                as returning_customers_revenue,
       COALESCE(refunded_customers.customers, 0)                                                               as refunded_customers_count,
       COALESCE(refunded_customers.refunded, 0)                                                                as customers_refunded,
       COALESCE(new_customers.week_of_year,
                returning_customers.week_of_year,
                refunded_customers.week_of_year)                                                               as period_id,
       'week'                                                                                                  as period_name,
       coalesce(new_customers.item_id,
                returning_customers.item_id,
                refunded_customers.item_id)                                                                    as item_id,
       coalesce(new_customers.city_id,
                returning_customers.city_id,
                refunded_customers.city_id)                                                                    as city_id
    from (select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as revenue,
             sum(quantity)       as items,
             count(*)            as customers
      from customers
      where status = 'shipped'
        and customer_id in (select customer_id from new_customers)
      group by week_of_year, city_id, item_id) new_customers
         full join
     (select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as revenue,
             sum(quantity)       as items,
             count(*)            as customers
      from customers
      where status = 'shipped'
        and customer_id in (select customer_id from returning_customers)
      group by week_of_year, city_id, item_id) returning_customers
     on new_customers.week_of_year = returning_customers.week_of_year
         and new_customers.item_id = returning_customers.item_id
         and new_customers.city_id = returning_customers.city_id
         full join
     (select week_of_year,
             city_id,
             item_id,
             sum(payment_amount) as refunded,
             sum(quantity)       as items,
             count(*)            as customers
      from customers
      where status = 'refunded'
        and customer_id in (select customer_id from refunded_customers)
      group by week_of_year, city_id, item_id) as refunded_customers
     on new_customers.week_of_year = refunded_customers.week_of_year
         and new_customers.item_id = refunded_customers.item_id
         and new_customers.city_id = refunded_customers.city_id
         ON CONFLICT mart.f_customer_retention DO UPDATE SET
         new_customers_count = EXCLUDED.new_customers_count,
         new_customers_revenue = EXCLUDED.new_customers_revenue, 
         returning_customers_count = EXCLUDED.returning_customers_count, 
         returning_customers_revenue = EXCLUDED.returning_customers_revenue, 
         refunded_customer_count = EXCLUDED.refunded_customer_count, 
         customers_refunded = EXCLUDED.customers_refunded ,  
         period_id = EXCLUDED.period_id, 
         period_name = EXCLUDED.period_name, 
         item_id = EXCLUDED.item_id,
         city_id = EXCLUDED.city_id;
     