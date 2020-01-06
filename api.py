#! /usr/bin/python3

from bottle import post, get, request, response, run, Bottle, template
import re
import os
import pandas as pd
import json
from joblib import dump, load

# Local Files
import src.mongodb_database as mdb

with open("models/gbrdefaultpickle_file.joblib", 'rb') as gbrpickle:
    gbr = load(gbrpickle)

def buildDataframe(userId):
    user, to_watch = mdb.getMoviestoWatch(userId)
    df = pd.DataFrame.from_dict(to_watch)
    df = df.join(pd.get_dummies(df['genres'].apply(pd.Series)))
    for e in range(0, 7):
        df[f'weekday_{e}'] = 0
    # df[f'weekday_{weekday}'] = 1
    df['user_rt_mean'] = user[0]['user_rt_mean']
    return df[['movieId', 'user_rt_mean', 'movie_rt_mean', 'popularity', 'weekday_0', 'weekday_1',
               'weekday_2', 'weekday_3', 'weekday_4', 'weekday_5', 'weekday_6',
               'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Documentary',
               'Drama', 'Family', 'Fantasy', 'Foreign', 'History', 'Horror', 'Music',
               'Mystery', 'Romance', 'Science Fiction', 'TV Movie', 'Thriller', 'War',
               'Western']]


def recommender(userId):
    df = buildDataframe(userId)
    X = df.drop(columns='movieId')
    prediction = gbr.predict(X)
    df['prediction'] = prediction
    X['movieId'] = df['movieId']
    df = df.sort_values('prediction', ascending=False)
    metadata = list(mdb.getMovieNames(list(df['movieId'][0:10])))
    results = []
    for e in metadata:
        result = {
            "id": e['id'],
            'name': e['original_title'],
            'predicted rating': round(float(df['prediction'].loc[df['movieId'] == e['id']]), 1)
        }
        results.append(result)
    return json.dumps({'userId': userId, 'predictions': results})


@get("/")
def index():
    return template('./html/index.html')


@get("/user/<userId>")
def recommendation(userId):
    try:
        userId = int(userId)
    except:
        raise ValueError(f'<userId> must be an integer.')
    return json.dumps(recommender(userId))


@get("/user/list/<cluster>")
def usersByCluster(cluster):
    try:
        cluster = int(cluster)
    except:
        raise ValueError(f'<cluster> must be an integer.')
    return json.dumps(mdb.getusersByCluster(cluster))


def main():
    port = int(os.getenv('PORT', 8080))
    host = os.getenv('IP', '0.0.0.0')
    run(host=host, port=port, debug=True)


if __name__ == '__main__':
    main()
