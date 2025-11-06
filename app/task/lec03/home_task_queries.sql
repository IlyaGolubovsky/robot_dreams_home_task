/* ================================================================
   Завдання на SQL до лекції 03.
   Домашнє завдання з 5 завданнями та їх результатами
   ================================================================ */


/* ================================================================
   ЗАВДАННЯ 1
   Вивести кількість фільмів в кожній категорії.
   Результат відсортувати за спаданням.
   ================================================================ */

SELECT
    c.name AS category_name,
    COUNT(f.film_id) AS film_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN film f ON fc.film_id = f.film_id
GROUP BY c.name
ORDER BY film_count DESC;

/* РЕЗУЛЬТАТ:
   ┌──────────────────┬────────────┐
   │ category_name    │ film_count │
   ├──────────────────┼────────────┤
   │ Drama            │       152  │
   │ Music            │       152  │
   │ Travel           │       151  │
   │ Games            │       150  │
   │ Children         │       150  │
   │ Foreign          │       150  │
   │ Sci-Fi           │       149  │
   │ Action           │       149  │
   │ Animation        │       148  │
   │ Family           │       147  │
   │ Classics         │       147  │
   │ New              │       147  │
   │ Sports           │       145  │
   │ Documentary      │       145  │
   │ Comedy           │       143  │
   │ Horror           │       142  │
   └──────────────────┴────────────┘

   Примітка: Усього фільмів у таблиці film: 1000
   Але один фільм може належати до кількох категорій,
   тому в результаті виходить 2367 записів (фільм-категорія)
*/


/* ================================================================
   ЗАВДАННЯ 2
   Вивести 10 акторів, чиї фільми брали на прокат найбільше.
   Результат відсортувати за спаданням.
   ================================================================ */

SELECT
    a.first_name || ' ' || a.last_name AS actor_name,
    COUNT(r.rental_id) AS rental_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film f ON fa.film_id = f.film_id
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY actor_name
ORDER BY rental_count DESC
LIMIT 10;

/* РЕЗУЛЬТАТ:
   ┌─────────────────────────┬────────────────┐
   │ actor_name              │ rental_count   │
   ├─────────────────────────┼────────────────┤
   │ SUSAN DAVIS             │            825 │
   │ GINA DEGENERES          │            753 │
   │ MATTHEW CARREY          │            678 │
   │ MARY KEITEL             │            674 │
   │ ANGELA WITHERSPOON      │            654 │
   │ WALTER TORN             │            640 │
   │ HENRY BERRY             │            612 │
   │ JAYNE NOLTE             │            611 │
   │ VAL BOLGER              │            605 │
   │ SANDRA KILMER           │            604 │
   └─────────────────────────┴────────────────┘

   Примітка: Використовуються зв'язки:
   actor → film_actor → film → inventory → rental
*/


/* ================================================================
   ЗАВДАННЯ 3
   Вивести категорію фільмів, на яку було витрачено найбільше грошей
   в прокаті
   ================================================================ */

SELECT
    c.name AS category_name,
    SUM(p.amount) AS total_revenue
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN film f ON fc.film_id = f.film_id
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN payment p ON r.rental_id = p.rental_id
GROUP BY c.name
ORDER BY total_revenue DESC
LIMIT 1;

/* РЕЗУЛЬТАТ:
   ┌─────────────────┬─────────────────┐
   │ category_name   │ total_revenue   │
   ├─────────────────┼─────────────────┤
   │ Foreign         │      10507.67   │
   └─────────────────┴─────────────────┘

   Примітка: Категорія "Foreign" принесла найбільше прибутків.
   Зв'язки: category → film_category → film → inventory → rental → payment
*/


/* ================================================================
   ЗАВДАННЯ 4
   Вивести назви фільмів, яких не має в inventory.
   Запит має бути без оператора IN
   ================================================================ */

SELECT
    f.title
FROM film f
WHERE NOT EXISTS (
    SELECT 1
    FROM inventory i
    WHERE i.film_id = f.film_id
);

/* РЕЗУЛЬТАТ: (42 рядки)
   ┌──────────────────────────────┐
   │ title                        │
   ├──────────────────────────────┤
   │ ALICE FANTASIA               │
   │ APOLLO TEEN                  │
   │ ARGONAUTS TOWN               │
   │ ARK RIDGEMONT                │
   │ ARSENIC INDEPENDENCE         │
   │ BOONDOCK BALLROOM            │
   │ BUTCH PANTHER                │
   │ CATCH AMISTAD                │
   │ CHINATOWN GLADIATOR          │
   │ CHOCOLATE DUCK               │
   │ COMMANDMENTS EXPRESS         │
   │ CROSSING DIVORCE             │
   │ CROWDS TELEMARK              │
   │ CRYSTAL BREAKING             │
   │ DAZED PUNK                   │
   │ DELIVERANCE MULHOLLAND       │
   │ FIREHOUSE VIETNAM            │
   │ FLOATS GARDEN                │
   │ FRANKENSTEIN STRANGER        │
   │ GLADIATOR WESTWARD           │
   │ GUMP DATE                    │
   │ HATE HANDICAP                │
   │ HOCUS FRIDA                  │
   │ KENTUCKIAN GIANT             │
   │ KILL BROTHERHOOD             │
   │ MUPPET MILE                  │
   │ ORDER BETRAYED               │
   │ PEARL DESTINY                │
   │ PERDITION FARGO              │
   │ PSYCHO SHRUNK                │
   │ RAIDERS ANTITRUST            │
   │ RAINBOW SHOCK                │
   │ ROOF CHAMPION                │
   │ SISTER FREDDY                │
   │ SKY MIRACLE                  │
   │ SUICIDES SILENCE             │
   │ TADPOLE PARK                 │
   │ TREASURE COMMAND             │
   │ VILLAIN DESPERATE            │
   │ VOLUME HOUSE                 │
   │ WAKE JAWS                    │
   │ WALLS ARTIST                 │
   └──────────────────────────────┘

   Примітка: Використаний NOT EXISTS замість IN
   Це більш ефективний спосіб перевірки відсутності записів
*/


/* ================================================================
   ЗАВДАННЯ 5
   Вивести топ 3 акторів, які найбільше з'являлись в категорії
   фільмів "Children"
   ================================================================ */

SELECT
    a.first_name || ' ' || a.last_name AS actor_name,
    COUNT(*) AS film_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN film f ON fa.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
WHERE c.name = 'Children'
GROUP BY actor_name
ORDER BY film_count DESC
LIMIT 3;

/* РЕЗУЛЬТАТ:
   ┌──────────────────┬────────────┐
   │ actor_name       │ film_count │
   ├──────────────────┼────────────┤
   │ RICHARD PENN     │          9 │
   │ EWAN GOODING     │          9 │
   │ SIDNEY CROWE     │          9 │
   └──────────────────┴────────────┘

   Примітка: Всі три актори мають однакову кількість фільмів
   в категорії "Children".
   Зв'язки: category → film_category → film → film_actor → actor
*/

/* ================================================================
   КІНЕЦЬ ДОМАШНЬОГО ЗАВДАННЯ
   ================================================================ */
