/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

with cat_qtt as (
    select
    category_id,
    count(distinct film_id) as films_qtt
    from film_category
    group by 1
    order by 2 desc
)
select
name,
films_qtt
from cat_qtt as cq
join category as ct
using(category_id);

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/


with films_rentals as (
    select
    rental_id,
    i.inventory_id,
    i.film_id,
    actor_id
    from rental as r
    join inventory as i
    on r.inventory_id = i.inventory_id
    join film_actor as fa
    on i.film_id = fa.film_id
)
select
actor_id,
CONCAT(last_name, ', ', first_name) AS actor_name,
count(distinct  rental_id) as rentals
from films_rentals as fr
join actor as a
using(actor_id)
group by 1,2
order by 3 desc
limit 10;

/*
3.
Вивести категорію фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

with rental_payments as (
    select
    p.rental_id,
    i.film_id,
    c.name,
    amount
    from rental as r
    join payment as p
    on r.rental_id = p.rental_id
    join inventory as i
    on r.inventory_id = i.inventory_id
    join film_category as f
    on i.film_id = f.film_id
    join category as c
    on c.category_id = f.category_id
)
select
name,
sum(amount) as total_spent
from rental_payments
group by 1
order by 2 desc
limit 1;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

select
distinct title
from film as f
left join inventory as i
on f.film_id = i.film_id
where i.film_id is null;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

with only_children_films as (
    select
    f.film_id,
    first_name,
    last_name
    from film as f
    join film_category as fc
    on f.film_id = fc.film_id
    join category as c
    on fc.category_id = c.category_id
    join film_actor as fa
    on f.film_id = fa.film_id
    join actor as a
    on fa.actor_id = a.actor_id
    where c.name = 'Children'
)
select
CONCAT(last_name, ', ', first_name) AS actor_name,
count(distinct  film_id) as films_qtt
from only_children_films
group by 1
order by 2 desc
limit 3;
