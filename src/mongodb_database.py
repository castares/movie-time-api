import os
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.json_util import loads, dumps
import pandas as pd

load_dotenv()
# Connection to Atlas Cluster
mongodbUrl = os.getenv("MONGODBURL")
# define Client, Database and collections on that database
client = MongoClient(mongodbUrl)
# define database
db = client['movie-recommender']
# define collections
users = db['users']
movies = db['movies']
metadata = db['metadata']


def addDocument(collection, document):
    # Standard function to insert documents on MongoDB
    return collection.insert_one(document)


def addUsersbulk(ratings, collection=users):
    ratings_grouped = ratings.groupby(['userId'])
    for userid, _ in ratings_grouped:
        if not list(users.find({"userId": userid})):
            user_avg_rating = ratings_grouped.get_group(
                userid)['user_rt_mean'].max()
            cluster = ratings_grouped.get_group(userid)['cluster'].max()
            movieids = [e for e in ratings_grouped.get_group(userid)[
                'movieId']]
            new_user = {
                'userId': int(userid),
                'user_rt_mean': user_avg_rating,
                'movies_rated': movieids,
                'cluster': int(cluster),
            }
            addDocument(collection, new_user)
            print(f'user {userid} added to collection')
        else:
            print(f'userId already exists. Try updating the document.')


def addMoviesBulk(ratings, users_genres, collection=movies):
    movies_grouped = ratings.groupby('movieId')
    for movieid, _ in movies_grouped:
        if not list(movies.find({"movieId": movieid})):
            movie_rt_mean = float(movies_grouped.get_group(
                movieid)['movie_rt_mean'].max())
            popularity = int(movies_grouped.get_group(
                movieid)['popularity'].max())
            clusters = list(
                map(int, movies_grouped.get_group(movieid)['cluster'].unique()))
            genres_list = list(users_genres.columns)
            genres = {genre: int(movies_grouped.get_group(
                movieid)[genre].max()) for genre in genres_list}
            new_movie = {
                'movieId': int(movieid),
                'movie_rt_mean': movie_rt_mean,
                'popularity': popularity,
                'clusters': clusters,
                'genres': genres,
            }
            addDocument(collection, new_movie)
            print(f'movie {movieid} added to collection')
        else:
            print(f'userId already exists. Try updating the document.')

# These are alternative versions of the functions to populate the collections movies and users
# without having to load the full dataset into memory.
#  They require a higher computing time and power than the ones above.
# def addUsersbulk(ratings, userid, collection=users):
#     user = ratings.loc[ratings['userId'] == userid]
#     user_rt_mean = float(user['user_rt_mean'].max())
#     movieids = [e for e in user['movieId'].compute()]
#     new_user = {
#         'userId': int(userid),
#         'user_rt_mean': user_rt_mean,
#         'movies_rated': movieids
#     }
#     addDocument(collection, new_user)
#     print(f'user {userid} added to collection {collection}')


# def addMoviesBulk(ratings, users_genres, movieid, collection=movies):
#     movie = ratings.loc[ratings['movieId'] == movieid].head(1)
#     clusters = list(
#         ratings['cluster'].loc[ratings['movieId'] == movieid].unique().compute())
#     movie_rt_mean = movie['movie_rt_mean']
#     popularity = movie['popularity']
#     genres_list = list(users_genres.columns)
#     genres = {genre: int(movie[genre]) for genre in genres_list}
#     weekdays = {str(weekday): int(movie[f'weekday_{weekday}'])
#                 for weekday in range(0, 7)}
#     new_movie = {
#         'movieId': int(movieid),
#         'movie_rt_mean': float(movie_rt_mean),
#         'popularity': int(popularity),
#         'clusters': clusters,
#         'genres': genres,
#         'weekdays': weekdays
#     }
#     addDocument(collection, new_movie)

def getUser(userId):
    return list(users.find({'userId': userId}, {'_id': 0}))


def getMoviestoWatch(userId):
    user = getUser(userId)
    watched = user[0]['movies_rated']
    to_watch = list(movies.find(
        {'clusters': {'$in': [user[0]['cluster']]}, 'movieId': {'$nin': watched}}))
    return user, to_watch


def getusersByCluster(cluster):
    return list(users.find({'cluster': cluster}, {'_id': 0, 'userId': 1, 'cluster': 1, 'user_rt_mean': 1}))


def getMovieNames(movieIds_list):
    return metadata.find({'id': {'$in': movieIds_list}}, {'id': 1, 'original_title': 1, 'overview': 1})


def main():
    print(list(metadata.find({'id': {'$in': [2, 3, 5]}}, {
        'id': 1, 'original_title': 1, 'overview': 1})))


if __name__ == "__main__":
    main()
