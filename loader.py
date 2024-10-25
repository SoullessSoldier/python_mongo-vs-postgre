import asyncio
import concurrent.futures
import logging
import uuid
from motor.motor_asyncio import AsyncIOMotorClient
import asyncpg
from random import randint, choice
import timeit


# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# Databases conf
POSTGRES_CONFIG = {
    'user': 'user',
    'password': 'password',
    'host': 'localhost',
    'port': '5432',
    'database': 'movies_db',
}

MONGO_URI = 'mongodb://localhost:27017'
MONGO_DB = 'movies_db'
MONGO_COLLECTION = 'movies'


class AsyncMongoClient:
    def __init__(self, uri):
        self.uri = uri
        self.client = None

    async def __aenter__(self):
        self.client = AsyncIOMotorClient(self.uri)
        return self.client

    async def __aexit__(self, exc_type, exc, tb):
        self.client.close()


# Fake data generation
def generate_movie_data(num_movies, num_users):
    movies = []
    user_id_list = [{'user_id': uuid.uuid4()} for _ in range(num_users)]
    for _ in range(num_movies):
        movie_id = uuid.uuid4()
        ratings = [{'user_id': user_id_list[randint(0, num_users-1)]['user_id'],
                    'rate': choice([-1, 1])}
                   for _ in range(randint(1, num_users))]

        bookmarks = [{'user_id': user_id_list[randint(0, num_users-1)]['user_id']} for _ in range(randint(1, num_users))]
        average_rating = sum(r['rate'] for r in ratings) / len(ratings)
        likes = sum(1 for r in ratings if r['rate'] == 1)
        dislikes = sum(1 for r in ratings if r['rate'] == -1)
        movies.append({
            'movie_id': movie_id,
            'ratings': ratings,
            'bookmarks': bookmarks,
            'average_rating': average_rating,
            'likes': likes,
            'dislikes': dislikes,
        })
    random_movie_id = movies[randint(0, num_movies - 1)]['movie_id']
    random_user_id = user_id_list[randint(0, num_users - 1)]['user_id']
    return movies, random_movie_id, random_user_id


# Postgres loader
async def load_data_postgres(pool, movie):
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute('''INSERT INTO movies (movie_id, average_rating, likes, dislikes) VALUES($1, $2, $3, $4)''',
                               movie['movie_id'], movie['average_rating'], movie['likes'], movie['dislikes'])
            for rating in movie['ratings']:
                await conn.execute('''INSERT INTO ratings (movie_id, user_id, rate) VALUES($1, $2, $3)''',
                                   movie['movie_id'], rating['user_id'], rating['rate'])
            for bookmark in movie['bookmarks']:
                await conn.execute('''INSERT INTO bookmarks (movie_id, user_id) VALUES ($1, $2)''',
                                   movie['movie_id'], bookmark['user_id'])


# Mongo loader
async def load_data_mongo(client, semaphore, movie):
    async with semaphore:
        try:
            # client = AsyncIOMotorClient(MONGO_URI)
            db = client[MONGO_DB]
            collection = db[MONGO_COLLECTION]
            movie['movie_id'] = str(movie['movie_id'])
            for rating in movie['ratings']:
                rating['user_id'] = str(rating['user_id'])
            for bookmark in movie['bookmarks']:
                bookmark['user_id'] = str(bookmark['user_id'])
            await collection.insert_one(movie)
        except Exception as e:
            print(f"Error inserting movie {movie['movie_id']} into MongoDB: {e}")


# Mongo reader
async def get_movie_from_mongo(client, movie_id):
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    movie = await collection.find({'movie_id': movie_id}).to_list()
    return movie


async def get_movies_by_user_from_mongo(client, user_id):
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Преобразуем user_id в строку, если он в формате UUID
    user_id_str = str(user_id)

    # Ищем фильмы, где user_id есть в ratings или bookmarks
    cursor = collection.find({
        '$or': [
            {'ratings.user_id': user_id_str},
            {'bookmarks.user_id': user_id_str}
        ]
    })

    movies = await cursor.to_list(length=None)
    # movies_count = await cursor.count()
    movies_count = len(movies)
    return movies_count


# Postgres reader
async def get_movie_from_postgres(pool, movie_id):
    async with pool.acquire() as conn:
        movie_query = '''SELECT movie_id, average_rating, likes, dislikes FROM movies WHERE movie_id = $1'''
        rating_query = '''SELECT user_id, rate FROM ratings WHERE movie_id = $1'''
        bookmark_query = '''SELECT user_id FROM bookmarks WHERE movie_id = $1'''

        movie = await conn.fetchrow(movie_query, movie_id)
        ratings = await conn.fetch(rating_query, movie_id)
        bookmarks = await conn.fetch(bookmark_query, movie_id)

        movie_data = {
            'movie_id': movie['movie_id'],
            'average_rating': movie['average_rating'],
            'likes': movie['likes'],
            'dislikes': movie['dislikes'],
            'ratings': [{'user_id': r['user_id'], 'rate': r['rate']} for r in ratings],
            'bookmarks': [{'user_id': b['user_id']} for b in bookmarks]
        }
        return movie_data


async def get_movies_by_user_from_postgres(pool, user_id):
    async with pool.acquire() as conn:
        rating_query = '''SELECT movie_id FROM ratings WHERE user_id = $1'''
        bookmark_query = '''SELECT movie_id FROM bookmarks WHERE user_id = $1'''

        rating_records = await conn.fetch(rating_query, user_id)
        bookmark_records = await conn.fetch(bookmark_query, user_id)

        movie_ids = set(
            record['movie_id'] for record in rating_records + bookmark_records)

        movie_data = []
        for movie_id in movie_ids:
            movie = await get_movie_from_postgres(pool, movie_id)
            movie_data.append(movie)

        return len(movie_data)


# Threading
async def load_data_concurrently(movies, load_function, is_async=False):
    if is_async:
        if load_function == load_data_postgres:
            async with asyncpg.create_pool(**POSTGRES_CONFIG,
                                           max_size=75) as pool:
                tasks = [asyncio.create_task(load_function(pool, movie)) for
                         movie in movies]
                await asyncio.gather(*tasks)
        else:
            client = AsyncIOMotorClient(MONGO_URI)
            semaphore = asyncio.Semaphore(100)
            tasks = [asyncio.create_task(load_function(client, semaphore, movie)) for movie in
                     movies]
            await asyncio.gather(*tasks)
            client.close()
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(load_function, movie) for movie in movies]
            concurrent.futures.wait(futures)


# Timeit func
async def measure_time(func, *args):
    start = timeit.default_timer()
    # result = asyncio.run(func(*args)) if asyncio.iscoroutinefunction(func) else func(*args)
    result = await func(*args) if asyncio.iscoroutinefunction(func) else func(
        *args)
    stop = timeit.default_timer()
    return stop - start, result


# Main
def main():
    num_movies = 100000
    num_users = 100

    # Generate data
    print("Generate data...")
    start = timeit.default_timer()
    movies, random_movie_id, random_user_id = generate_movie_data(num_movies, num_users)
    stop = timeit.default_timer()
    print(f"Generating data takes {stop-start:.2f} seconds")

    # Mongo work
    print("Work with Mongo loader")
    mongo_time, _ = asyncio.run(measure_time(load_data_concurrently, movies, load_data_mongo, True))
    print(f"Work with Mongo loader takes {mongo_time:.2f} seconds")

    # Postgres work
    print("Work with Postgres loader")
    postgres_time, _ = asyncio.run(measure_time(load_data_concurrently, movies, load_data_postgres, True))
    print(f"Work with Postgres loader takes {postgres_time:.2f} seconds")

    print("===========")

    async def mongo_operations():
        # Mongo get movie
        # client = AsyncIOMotorClient(MONGO_URI)
        async with AsyncMongoClient(MONGO_URI) as client:
            try:
                print("Work with Mongo get movie by movie_id")
                start = timeit.default_timer()
                mongo_movie = await get_movie_from_mongo(client, str(random_movie_id))
                stop = timeit.default_timer()
                print("mongo_movie", mongo_movie[0]['movie_id'])
                print(f"Work with Mongo get movie by movie_id takes {stop-start:.2f} seconds")
                print("-----------")
                print("Work with Mongo get movies by user_id")
                start = timeit.default_timer()
                mongo_result = await get_movies_by_user_from_mongo(client, str(random_user_id))
                stop = timeit.default_timer()
                print('Number of movies: {}'.format(mongo_result))
                print(f"Work with Mongo get movies by user_id takes {stop-start:.2f} seconds")
            except Exception as e:
                print(f"An error occurred: {e}")
        print("===========")

    asyncio.run(mongo_operations())

    async def postgres_operations():
        pool = await asyncpg.create_pool(**POSTGRES_CONFIG, max_size=10)

        print("Work with Postgres get movie by movie_id")
        postgres_time, movie = await measure_time(get_movie_from_postgres, pool,
                                            random_movie_id)
        print("postgres_movie", movie['movie_id'])
        print(f"Work with Postgres get movie takes {postgres_time:.2f} seconds")
        print("-----------")
        print("Work with Postgres get movies by user_id")
        postgres_user_time, user_movies_length = await measure_time(
            get_movies_by_user_from_postgres, pool, random_user_id)
        print("Number of movies:", user_movies_length)
        print(
            f"Work with Postgres get movies by user_id takes {postgres_user_time:.2f} seconds")

        await pool.close()

    asyncio.run(postgres_operations())


if __name__ == '__main__':
    main()

