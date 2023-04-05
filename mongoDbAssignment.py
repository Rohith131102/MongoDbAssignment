import pymongo 
import subprocess
import pprint

client = pymongo.MongoClient('mongodb://localhost:27017/')


dbName = "AssigmentDb"
collectionNames = ["comments","movies","theaters","users"]

db = client[dbName]


commentsCollection=db[collectionNames[0]]
moviesCollection=db[collectionNames[1]]
theatersCollection=db[collectionNames[2]]
usersCollection=db[collectionNames[3]]


class createDatabaseCollections():
    def createDatabase():
        subprocess.run(["mongo", "--eval", f"db.createDatabase('{dbName}')"])
    # Create collection using MongoDB shell command
    def createCollections():
        for i in range(len(collectionNames)):
            subprocess.run(["mongo", "--eval", f"db.createCollection('{collectionNames[i]}')", dbName])
        
    def loadData():
         
    # Define path to JSON file containing data to import
        json_file_path = "/Users/rohith_boodireddy/Downloads/sample_mflix/comments.json"
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", dbName, "--collection", collectionNames[0], "--file", json_file_path])
        json_file_path = "/Users/rohith_boodireddy/Downloads/sample_mflix/movies.json"
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", dbName, "--collection", collectionNames[1], "--file", json_file_path])
        json_file_path = "/Users/rohith_boodireddy/Downloads/sample_mflix/theaters.json"
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", dbName, "--collection", collectionNames[2], "--file", json_file_path])
        json_file_path = "/Users/rohith_boodireddy/Downloads/sample_mflix/users.json"
        # Import data using MongoDB shell command
        subprocess.run(["mongoimport", "--db", dbName, "--collection", collectionNames[3], "--file", json_file_path])

def insert_record_dict(collection,data_dict:dict):
       collection.insert_one(data_dict)

def top10UserWithMaxComment():
        pipeline=[
            {"$group":{"_id":"$name","count":{"$sum":1} }},
            {"$sort":{"count":-1}},
            {"$limit":10},
            {"$project" : {"_id":0,"User":"$_id","count":1}}
        ]
        pprint(list(commentsCollection.aggregate(pipeline)))

def top10MoviesWithMaxComment():
        pipeline=[
            {"$group":{"_id":"$movie_id","count":{"$sum":1} }},
            {"$sort":{"count":-1}},
            {"$limit":10}
        ]
        for doc in commentsCollection.aggregate(pipeline):
            print("Movie: " + moviesCollection.find_one({ "_id":doc["_id"] })['title'] + ", No of Comments: " + str(doc['count']))


def monthWiseComments(year):
    pipeline=[
        { "$project": {"year":{"$year":"$date"} , "month":{"$month":"$date"} }},
        {"$match" : { "year":year }},
        {"$group" : {"_id" : "$month", "count" : {"$sum":1}}},
        {"$project": {"month":"$_id","count":1,"_id":0 }},
        {"$sort" : {"month":1 }}
    ]
    pprint(list(commentsCollection.aggregate(pipeline)))

def findTop_N_MoviesWithTheHighestIMDBRating(N:int):
    pipeline=[
        {"$match": {"imdb.rating":{"$ne":""}}},
        {"$sort":{ "imdb.rating":-1}},
        {"$limit":N},
        {"$project":{"_id":0,"title":1,"imbd.rating":1}}
        ]
    pprint(list(moviesCollection.aggregate(pipeline)))

def findTop_N_MoviesWithTheHighestIMDBRatingInYear(year):
    pipeline=[
        {"$match": { "$and":[ {"imdb.rating":{"$ne":""}},{"year":year} ]}},
        {"$sort":{ "imdb.rating":-1}},
        {"$limit":int(input("Enter the value of N: "))},
        {"$project":{"_id":0,"title":1,"imbd.rating":1,"year":1}}
        ]
    pprint(list(moviesCollection.aggregate(pipeline)))


def FindTop_N_MoviesWithTheHighestIMDBRatingAndVotesGreaterThan1000(N:int):
    pipeline=[
            {"$match": { "$and":[ {"imdb.rating":{"$ne":""}},{"imdb.votes":{"$gte":1000}} ]}},
            {"$sort":{ "imdb.rating":-1}},
            {"$limit":N},
            {"$project":{"_id":0,"title":1,"imbd.rating":1,"imdb.votes":1}}
            ]
    pprint(list(moviesCollection.aggregate(pipeline)))


def FindTop_N_MoviesWithTitleMatchingPatternSortedByHighestTomatoesRatings(N:int):
    pipeline=[
            {"$match": { "title": {"$regex":input("Enter the regex pattern: "),"$options":"i"}}},
            {"$sort":{ "tomatoes.viewer.rating":-1}},
            {"$limit":N},
            {"$project":{"_id":0,"title":1,"rating":"$tomatoes.viewer.rating"}}
        ]
    pprint(list(moviesCollection.aggregate(pipeline)))

def FindTop_N_DirectorsWithMaximumNoOfMovies(N:int):
    pipeline=[
            {"$unwind":"$directors"},
            {"$group":{"_id":"$directors","noOfMovies":{"$sum":1}}},
            {"$sort":{"noOfMovies":-1}},
            {"$limit":N}
            ]
    pprint(list(moviesCollection.aggregate(pipeline)))

def FindTop_N_directorsWithMaximumNoOfMoviesInAnYear(year,N:int):
    pipeline=[
        {"$match":{"year":year}},
        {"$unwind":"$directors"},
        {"$group":{"_id":"$directors","noOfMovies":{"$sum":1}}},
        {"$sort":{"noOfMovies":-1}},
        {"$limit":N}
        ]
    pprint(list(moviesCollection.aggregate(pipeline)))

def FindTop_N_directorsWithMaximumNoOfMoviesInGivenGenre(genre,N:int):
        pipeline=[
                {"$match":{"genres":genre}},
                {"$unwind":"$directors"},
                {"$group":{"_id":"$directors","noOfMovies":{"$sum":1}}},
                {"$sort":{"noOfMovies":-1}},
                {"$limit":N}
            ]
        pprint(list(moviesCollection.aggregate(pipeline)))

def FindTop_N_actorsWithMaximumNoOfMovies(N:int):
    pipeline=[
            {"$unwind":"$cast"},
            {"$group":{"_id":"$cast","noOfMovies":{"$sum":1}}},
            {"$sort":{"noOfMovies":-1}},
            {"$limit":N}
        ]
    pprint(list(moviesCollection.aggregate(pipeline)))


def FindTop_N_actorsWithMaximumNoOfMoviesInGivenYear(year,N:int):
    pipeline=[
                {"$match":{"year":year}},
                {"$unwind":"$cast"},
                {"$group":{"_id":"$cast","noOfMovies":{"$sum":1}}},
                {"$sort":{"noOfMovies":-1}},
                {"$limit":N}
            ]
    pprint(list(moviesCollection.aggregate(pipeline)))


def FindTop_N_actorsWithMaximumNoOfMoviesOInGiveGenre(genre,N:int):
    pipeline=[
            {"$match":{"genres":genre}},
            {"$unwind":"$cast"},
            {"$group":{"_id":"$cast","noOfMovies":{"$sum":1}}},
            {"$sort":{"noOfMovies":-1}},
            {"$limit":N}
        ]
    pprint(list(moviesCollection.aggregate(pipeline)))

def top_N_MoviesForEveryGenre():
    pipe=[
        {"$unwind":"$genres"},
        {"$group":{"_id":"$genres"}}
    ]
    for i in list(moviesCollection.aggregate(pipe)):
        genre=i['_id']
        print("Genre: "+genre)
        pipeline=[
            {"$unwind":"$genres"},
            {"$match":{"genres":genre}},
            {"$sort":{"imdb.rating":-1}},
            {"$match":{"imdb.rating":{"$ne":""}}},
            {"$project":{"_id":0,"title":1,"rating":"$imdb.rating"}},
            {"$limit":int(input("Enter the value of N: "))}
        ] 
        pprint(list(moviesCollection.aggregate(pipeline))) 

def top10CitiesMostTheaters():
    pipeline=[
        {"$group":{"_id":"$location.address.city","cnt":{"$sum":1}}},
        {"$sort":{"cnt":-1}},
        {"$limit":10}
    ]
    pprint(list(theatersCollection.aggregate(pipeline)))


def top10theatersNear(coordinates):
    theatersCollection.create_index([("location.geo", "2dsphere")])
    pprint(list(theatersCollection.find(
    {
    "location.geo": {
        "$near": {
        "$geometry": {
            "type": "Point" ,
            "coordinates": coordinates
        }}
    }
    }).limit(10)))

print("Question-1.1 Find top 10 users who made the maximum number of comments")
top10UserWithMaxComment()
print("Question-1.2 Find top 10 movies who made the maximum number of comments")
top10MoviesWithMaxComment()
print("Question-1.3 Given a year find the total number of comments created each month in that year")
monthWiseComments(int(input("Enter the year: ")))
print("Question-2.1.1 Find top `N` movies with the highest IMDB rating")
findTop_N_MoviesWithTheHighestIMDBRating(int(input("enter a number")))
print("Question-2.1.2 Find top `N` movies with the highest IMDB rating in a given year")
findTop_N_MoviesWithTheHighestIMDBRatingInYear(int(input("Enter the year: ")))
print("Question-2.1.3 Find top `N` movies with the highest IMDB rating, votes more than 1000")
FindTop_N_MoviesWithTheHighestIMDBRatingAndVotesGreaterThan1000(int(input("enter a number")))
print("Question-2.1.4 with title matching a given pattern sorted by highest tomatoes ratings")
FindTop_N_MoviesWithTitleMatchingPatternSortedByHighestTomatoesRatings(int(input("Enter the year")))
print("Question-2.2.1 Find top `N` directors who created the maximum number of movies")
FindTop_N_DirectorsWithMaximumNoOfMovies(int(input("Enter a number")))
print("Question-2.2.2 Find top `N` directors who created the maximum number of movies in given year")
FindTop_N_directorsWithMaximumNoOfMoviesInAnYear(int(input("enter a number")))
print("Question-2.2.3 Find top `N` directors who created the maximum number of movies in given genre")
FindTop_N_directorsWithMaximumNoOfMoviesInGivenGenre(input("Enter the genre: "),int(input("Enter the number")))
print("Question-2.3.1 Find top `N` actors who created the maximum number of movies")
FindTop_N_actorsWithMaximumNoOfMovies(int(input("Enter a number")))
print("Question-2.3.2 Find top `N` actors who created the maximum number of movies in given year")
FindTop_N_actorsWithMaximumNoOfMoviesInGivenYear(int(input("Enter the year: ")),int(input("enter a number")))
print("Question-2.3.3 Find top `N` actors who created the maximum number of movies in given genre")
FindTop_N_actorsWithMaximumNoOfMoviesOInGiveGenre(input("Enter the genre: "),int(input("Enter a number")))
print("Question-2.4 Find top `N` movies for each genre with the highest IMDB rating")
top_N_MoviesForEveryGenre()
print("Question-3.1 Top 10 cities with the maximum number of theatres")
top10CitiesMostTheaters()
print("Question-3.2 Top 10 theatres nearby given coordinates")
top10theatersNear([float(input("Enter Longitude: ")),float(input("Enter Latitude: "))]) 
