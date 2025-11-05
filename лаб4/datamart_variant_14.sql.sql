-- Витрина данных для анализа рейтингов фильмов
-- Вариант задания №14
-- Создает VIEW на основе обогащенных данных из stg_movies_ratings

DROP VIEW IF EXISTS movies_ratings_datamart;

CREATE VIEW movies_ratings_datamart AS
SELECT 
    movie_title,
    release_year,
    genre,
    director_name,
    average_rating,
    num_votes,
    -- Дополнительные расчетные поля для аналитики
    CASE 
        WHEN average_rating >= 8.0 THEN 'Высокий рейтинг'
        WHEN average_rating >= 6.0 THEN 'Средний рейтинг'
        ELSE 'Низкий рейтинг'
    END as rating_category,
    -- Группировка по десятилетиям для временного анализа
    FLOOR(release_year / 10) * 10 as release_decade
FROM 
    stg_movies_ratings
WHERE
    -- Фильтруем данные с корректными рейтингами и годами
    average_rating IS NOT NULL 
    AND release_year BETWEEN 1900 AND 2024
    AND num_votes >= 1000  -- Только фильмы с достаточным количеством оценок
    AND genre IS NOT NULL
    AND director_name IS NOT NULL;

-- Комментарий к витрине
COMMENT ON VIEW movies_ratings_datamart IS 
'Обогащенная витрина данных для анализа рейтингов фильмов. 
Содержит информацию о фильмах, их рейтингах, жанрах и режиссерах.
Готова для использования в дашбордах и аналитических отчетах.';

-- Дополнительное представление для агрегированных данных по жанрам
DROP VIEW IF EXISTS genre_aggregates;

CREATE VIEW genre_aggregates AS
SELECT 
    genre,
    COUNT(*) as movie_count,
    ROUND(AVG(average_rating), 2) as avg_rating,
    SUM(num_votes) as total_votes,
    ROUND(AVG(num_votes), 0) as avg_votes_per_movie,
    MIN(average_rating) as min_rating,
    MAX(average_rating) as max_rating
FROM 
    stg_movies_ratings
WHERE 
    average_rating IS NOT NULL
    AND genre IS NOT NULL
GROUP BY 
    genre
HAVING 
    COUNT(*) >= 3  -- Только жанры с минимум 3 фильмами
ORDER BY 
    avg_rating DESC;

COMMENT ON VIEW genre_aggregates IS 
'Агрегированная статистика по жанрам фильмов.
Полезно для сравнения жанров по средним рейтингам и популярности.';

-- Представление для анализа режиссеров
DROP VIEW IF EXISTS director_analysis;

CREATE VIEW director_analysis AS
SELECT 
    director_name,
    COUNT(*) as movies_count,
    ROUND(AVG(average_rating), 2) as avg_director_rating,
    SUM(num_votes) as total_votes,
    MIN(release_year) as first_movie_year,
    MAX(release_year) as last_movie_year,
    ROUND(AVG(num_votes), 0) as avg_votes_per_movie,
    -- Рассчитываем стабильность рейтингов режиссера
    ROUND(STDDEV(average_rating), 2) as rating_std_dev
FROM 
    stg_movies_ratings
WHERE 
    director_name IS NOT NULL
    AND average_rating IS NOT NULL
GROUP BY 
    director_name
HAVING 
    COUNT(*) >= 2  -- Только режиссеры с минимум 2 фильмами
ORDER BY 
    avg_director_rating DESC,
    movies_count DESC;

COMMENT ON VIEW director_analysis IS 
'Анализ режиссеров по количеству фильмов, средним рейтингам и популярности.
Полезно для выявления наиболее успешных режиссеров.';

-- Представление для временного анализа
DROP VIEW IF EXISTS yearly_trends;

CREATE VIEW yearly_trends AS
SELECT 
    release_year,
    COUNT(*) as movies_count,
    ROUND(AVG(average_rating), 2) as avg_yearly_rating,
    SUM(num_votes) as total_yearly_votes,
    ROUND(AVG(num_votes), 0) as avg_votes_per_movie
FROM 
    stg_movies_ratings
WHERE 
    release_year BETWEEN 2000 AND 2024  -- Фокус на современном кинематографе
    AND average_rating IS NOT NULL
GROUP BY 
    release_year
HAVING 
    COUNT(*) >= 5  -- Только годы с достаточным количеством фильмов
ORDER BY 
    release_year DESC;

COMMENT ON VIEW yearly_trends IS 
'Тренды кинематографа по годам: количество фильмов, средние рейтинги и популярность.
Полезно для анализа изменений в киноиндустрии с течением времени.';