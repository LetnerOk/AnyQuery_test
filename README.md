<details>
  <summary>Условие тестового задания</summary>

У нас есть 2 таблицы с данными описанные ниже. Они покрывают события типа:
* Поисковые события
* Клики на сайтах
  + Клик на товар (eventType - product_click)
  + Добавление в корзину (eventType - card_add_event)
  + Клик на коризну (eventType - card_click_event)
  + Клик на завершение оплаты (eventType - order_click)

Они хранятся в базе данных Clickhouse (но можно использовать любой sql диалект)
Задача: Вам необходимо предложить метрики, методы их расчета и визуализации на основе
имеющихся данных. Доступные инструменты для использования:
* SQL
* Python
* BI системы (можно описать график который использовали бы для визуализации, пример - линейный график с осями.....)
Кол-во метрик и глубину погружения определяете сами.
</details>


Перед началом работы с данными сделаем несколько предположений о системе поиска.
Предположим, что существует два "вида" поискового движка. Обычный поиск - по ключевым
словам, и поиск аналогов, если не срабатывает первоначальный поиск. Поле `searchWorked`
принимает булево значение `true`, если срабатывает поиск по введенному запросу, значение
`false`, если товаров не найдено по ключевым словам. Если `searchWorked = false`, то
подключается поиск ZeroQuery, то есть поиск аналогов. Поле `isZeroQuery` также принимает
булевы значения: `true`, если срабатывает поиск аналогов и находятся товары. Если в обоих
полях `isZeroQuery` и `searchWorked` стоит `false`, то оба поиска не дали результатов.


По сделанным предположениям и данным интересно будет оценить и посмотреть следующие
метрики:
1. Воронка продаж и конверсия клика в покупку, клика в добавление товара в
корзину
2. Общее число запросов, в том числе уникальных
3. Число запросов, по которым не нашлось товаров, в том числе уникальных
4. Число запросов, по которым сработал поиск аналогов ZeroQuery
5. Топ популярных запросов за весь период, по неделям или месяцам
6. Топ популярных товаров
7. Нулевые запросы по популярности


Разберем определение этих метрик с использованием СУБД PostgreSQL.

## 1. Воронка продаж и конверсия в покупку


На первом этапе для поиска проблемных мест необходимо построить воронку продаж и
посчитать конверсию пользователей от клика на товар до совершения оплаты
Воронку можно посчитать двумя способами, сформировав таблицу, как по вертикали, так и по
горизонтали.


При горизонтальном соединении будем выдавать в подзапросах количество уникальных пользователей для каждого типа ивента и соединять их в одну таблицу с помощью `LEFT JOIN`. 
Например, так:


```sql
WITH product_click AS
(
SELECT
    DISTINCT ON (userGUID)
    userGUID,
    EventType as stage1
FROM Widgetclicks
WHERE EventType = 'product_click'
),

card_add AS
(
SELECT
    DISTINCT ON (userGUID)
    userGUID,
    EventType as stage2
FROM Widgetclicks
WHERE EventType = 'card_add_event'
),

card_click AS
(
SELECT
    DISTINCT ON (userGUID)
    userGUID,
    EventType as stage3
FROM Widgetclicks
WHERE EventType = 'card_click_event'
),

order_click AS
(
SELECT
    DISTINCT ON (userGUID)
    userGUID,
    EventType as stage4
FROM Widgetclicks
WHERE EventType = 'order_click'
)

SELECT
    count(stage1) AS count_product_click,
    count(stage2) AS count_card_add,
    count(stage3) AS count_card_click,
    count(stage4) AS order_click
FROM product_click
LEFT JOIN card_add
USING (userGUID)
LEFT JOIN card_click
USING (userGUID)
LEFT JOIN order_click
USING (userGUID)
```


На выходе получим таблицу типа:

![](/figures_for_test/table1.png)


При формировании таблицы по вертикали в CTE будем объединять подзапросы уникальных
пользователей по разным ивентам с помощью `UNION ALL`, в основном запросе выведем
общее количество уникальных юзеров с сортировкой по ивентам от клика на продукт до
покупки.


На выходе получим таблицу типа:

![](/figures_for_test/table2.png)


Выгрузив таблицу из СУБД в виде csv файла, можно загрузить его BI платформу, к примеру,
**Apache Superset** и визуализировать графиком **Funnel Chart**.

**Конверсию** можно получить, добавив в основной запрос ее расчет:

```sql
SELECT
    COUNT(stage1) AS count_product_click,
    COUNT(stage2) AS count_card_add,
    COUNT(stage3) AS count_card_click,
    COUNT(stage4) AS order_click,
	  ROUND(COUNT(stage4) * 100.00 / COUNT(stage1), 2) AS conv_product_click_to_order_click,
    ROUND((COUNT(stage2)) * 100.00 / COUNT(stage1), 2) AS conv_product_click_to_card_add,
    ROUND(COUNT(stage4) * 100.00 / COUNT(stage3), 2) AS conv_card_click_to_order_click
FROM product_click
LEFT JOIN card_add
USING (userid)
LEFT JOIN card_click
USING (userid)
LEFT JOIN order_click
USING (userid)```


Можно рассчитать конверсию в динамике, например, по дням. Для этого в подзапросах
необходимо группировать счет уникальных юзеров по дням, используя, например
`TO_CHAR(timestamp, 'YYYY-MM-DD') AS day`. В основном запросе добавить группировку и
сортировку по `day`. Тогда можно будет построить линейный график зависимости конверсии
дням.


## 2. Общее число запросов, в том числе уникальных


Число запросов будем оценивать по полю searchTerm, где уже сработала автокоррекция и
исправление опечаток. Также по этому полю можно посчитать количество уникальных
запросов.


### Общее число запросов:


```sql
SELECT
    COUNT(searchTerm) AS total_search
FROM Searches
```

### Общее число уникальных запросов:


```sql
SELECT
    COUNT(DISTINCT searchTerm) AS total_unique_search
FROM Searches
```


## 3. Число запросов, по которым не нашлось товаров, в том числе уникальных («нулевые» запросы)


Будем считать, что товаров не нашлось по запросу пользователя, если оба признака
`searchWorked` и `isZeroQuery` = `false`. Запрос будет выглядеть так:


```sql
SELECT
    COUNT(searchTerm) AS zero_search
FROM Searches
WHERE searchWorked = false AND isZeroQuery = false
```


## 4. Число запросов, по которым сработал поиск аналогов ZeroQuery

Если поиск по ключевому слову не дал результат, запускается умный движок поиска ZeroQuery.
Запрос для подсчета поиска аналогов будет выглядеть так:


```sql
SELECT
    COUNT(searchTerm) AS search_isZeroQuery
FROM Searches
WHERE searchWorked = false AND isZeroQuery = true
```


Число запросов с разной фильтрацией (запросы с результатом, «нулевые» запросы, запросы
ZeroQuery) можно наглядно отобразить на **Pie Chart**, например, на той же платформе **Apache
Superset**, выгрузив таблицу следующего вида:


![](/figures_for_test/table3.png)


`SELECT` этой таблицы будет выглядеть следующим образом:


```sql
SELECT
    'zero_search' AS search_type,
    COUNT(searchTerm) AS count_search
FROM Searches
WHERE searchWorked = false AND isZeroQuery = false

UNION ALL

SELECT
    'search_isZeroQuery' AS search_type,
    COUNT(searchTerm) AS count_search
FROM Searches
WHERE searchWorked = false AND isZeroQuery = true

UNION ALL

SELECT
    'search_result' AS search_type,
    COUNT(searchTerm) AS count_search
FROM Searches
WHERE searchWorked = true
```


Число запросов интересно будет посмотреть в динамике по дням, используя **stacked bar
chart**, отображая общее число запросов и «нулевые». Визуализировать график можно как в **BI
системе**, так и в **Python** с использованием библиотеки **Matplotlib**.


Приведу пример обработки данных и построения bar chart в Python.

```python
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Читаем таблицу и преобразуем в датафрейм
df_search_csv = pd.read_csv('Searches.csv')
df_search = pd.DataFrame(df_search_csv)

# Убеждаемся, что дата дана в формате datetime и приводим ее ко дню
df_search['timestamp'] = pd.to_datetime(df_search['timestamp'])
df_search['dateday'] = df_search['timestamp'].dt.date
df_search_sorted = df_search.sort_values(by='dateday', ascending=True)

# Подсчет общего количества searchTerm по дням
total_search = df_search_sorted.groupby('dateday')['searchterm'].count().reset_index(name='count_total_search')
# Фильтрация по условиям
filtered_df_search = df_search_sorted[(df_search_sorted['searchworked'] == False) & (df_search_sorted['iszeroquery'] == False)]

# Подсчет количества "нулевых" search по дням
null_df_search = filtered_df_search.groupby('dateday')['searchterm'].count().reset_index(name='count_null_search')

# Объединение результатов в одну таблицу
total_null_search = pd.merge(total_search, null_df_search, on='dateday', how='left').fillna(0)

# Вычисление оставшейся части (нефильтрованных значений)
total_null_search["difference"] = total_null_search["count_total_search"] - total_null_search["count_null_search"]

# Построение stacked bar chart
plt.figure(figsize=(10, 5))

# Настройка оси X для отображения только дат (без времени)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
# Ограничение количество меток
plt.gca().xaxis.set_major_locator(mdates.DayLocator())

# Первый компонент: "нулевые" запросы
plt.bar(total_null_search['dateday'], total_null_search['count_null_search'], label='null_search', color='coral')

# Второй компонент: разница между полным числом запросов и "нулевыми"
plt.bar(total_null_search['dateday'], total_null_search['difference'], bottom=total_null_search['count_null_search'], color='blue')

plt.title('Поисковые запросы по дням', fontsize=16) # заголовок
plt.xlabel('Дата', fontsize=14) # ось абсцисс
plt.xticks(rotation=45)
plt.ylabel('Количество запросов', fontsize=14) # ось ординат

```

На выходе получаем такой график:


![](/figures_for_test/bar_chart.png)


## 5. Топ популярных запросов


```sql
SELECT
    searchTerm,
    COUNT(searchTerm)
FROM Searches
GROUP BY productId
ORDER BY COUNT(productId) DESC
```


С таким селектом можно получить список запросов по популярности. Для выявления
трендовых запросов можно проследить количество запросов за предыдущие недели и
посчитать, есть ли прирост в частоте запроса. Отфильтруем данные за последние две недели.
Информация лучше будет восприниматься в табличном виде. Выгрузив данные в формате
csv, можно загрузить на дашборд, например, на платформе **Apache Superset**.


Пример запроса:


```sql
WITH case_Searches AS
(
SELECT
    searchTerm,
    SUM(
        CASE
        WHEN timestamp >= DATE_TRUNC('week' , NOW()) - INTERVAL '1 week'
            AND timestamp < DATE_TRUNC('week' , NOW())
        THEN 1 ELSE 0 END) AS previous_week,
    SUM(
        CASE
        WHEN timestamp >= DATE_TRUNC('week' , NOW()) - INTERVAL '2 weeks'
            AND timestamp < DATE_TRUNC('week' , NOW()) - INTERVAL '1 week'
        THEN 1 ELSE 0 END) AS two_weeks_ago
FROM Searches
WHERE timestamp >= DATE_TRUNC('week' , NOW()) - INTERVAL '2 weeks' -- Фильтруем только последние две недели
GROUP BY searchTerm
)
SELECT
    searchTerm,
    two_weeks_ago,
    previous_week,
    ROUND((previous_week - two_weeks_ago) * 100.00 / two_weeks_ago, 0) AS growth
ORDER BY searchTerm;
```


## 6. Топ популярных товаров


Популярность товаров можно отследить по полю `productId`:

```sql
SELECT
    productId,
    COUNT(productId)
FROM Widgetclicks
GROUP BY productId
ORDER BY COUNT(productId) DESC
```

Также можно сгруппировать по `eventType` и отслеживать популярность товаров по типу события. Данные удобно просматривать в табличном виде. Можно выгрузить в формате csv и
отобразить на Дашборде, например на платформе Apache Superset.
