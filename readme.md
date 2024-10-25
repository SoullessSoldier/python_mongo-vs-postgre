# Сравнение хранилищ Postgres и MongoDB

Записываем 100k записей асинхронно и многопоточно.

Для записи в Монго через семафор определяем число потоков в 100
В Постгрес по дефолту видно (SHOW max_connections), что можно также \
выставить в функции asyncpg.create_pool max_size=100

Очистка коллекции в Монго shell: db.movies.deleteMany({})

### Структура БД
#### Mongo:
```json
{
    "movie_id": "UUID",
    "average_rating": 0,
    "likes": 0,
    "dislikes": 0,
    "ratings": [
        {
            "user_id": "UUID",
            "rate": 1
        }
    ],
    "bookmarks": [
        {
            "user_id": "UUID"
        }
    ]
}



```
Также в коллекции movies создан индекс по полю movie_id 

#### PostgreSQL
```postgresql
create database movie_db;


-- public.movies определение

-- Drop table

-- DROP TABLE public.movies;

CREATE TABLE public.movies (
	movie_id uuid NOT NULL,
	average_rating float8 DEFAULT 0 NULL,
	likes int4 NULL,
	dislikes int4 NULL,
	CONSTRAINT movies_pkey PRIMARY KEY (movie_id)
);

-- ratings table
-- public.ratings определение

-- Drop table

-- DROP TABLE public.ratings;

CREATE TABLE public.ratings (
	id serial4 NOT NULL,
	movie_id uuid NULL,
	user_id uuid NULL,
	rate int4 NULL,
	CONSTRAINT ratings_pkey PRIMARY KEY (id),
	CONSTRAINT ratings_rate_check CHECK ((rate = ANY (ARRAY[1, '-1'::integer])))
);
CREATE INDEX ratings_movie_id_idx ON public.ratings USING btree (movie_id);
CREATE INDEX ratings_user_id_idx ON public.ratings USING btree (user_id);


-- public.ratings внешние включи

ALTER TABLE public.ratings ADD CONSTRAINT ratings_movie_id_fkey FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id);

-- bookmarks table
-- public.bookmarks определение

-- Drop table

-- DROP TABLE public.bookmarks;

CREATE TABLE public.bookmarks (
	id serial4 NOT NULL,
	movie_id uuid NULL,
	user_id uuid NULL,
	CONSTRAINT bookmarks_pkey PRIMARY KEY (id)
);
CREATE INDEX bookmarks_movie_id_idx ON public.bookmarks USING btree (movie_id);
CREATE INDEX bookmarks_user_id_idx ON public.bookmarks USING btree (user_id);


-- public.bookmarks внешние включи

ALTER TABLE public.bookmarks ADD CONSTRAINT bookmarks_movie_id_fkey FOREIGN KEY (movie_id) REFERENCES public.movies(movie_id);
```

Полезняшки Postgresql
```postgresql
--Очистка таблиц TRUNCATE
truncate bookmarks, ratings, movies cascade;

--Считаем количество записей
select count(*) from movies m; 
```
