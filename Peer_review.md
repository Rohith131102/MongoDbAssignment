
## MONGODB Assignment Peer review


### Rithish Assignment Review

created a Python application to connect to the MongoDB server by using the Pymongo library and loaded the bulk data to the MongoDB database from the command line using the Mongoimport command.

##### Find top 10 users who made the maximum number of comments 
used aggregation pipeline to group the comments by the name field, count the number of comments made by each user, sort the results in descending order of the comment count, limit the output to 10 documents, and project only the User and count fields while excluding the _id field. The resulting documents are printed using the pprint() function.

##### Find top 10 movies with most comments
used aggregation pipeline to group the comments by the movie_id field, count the number of comments for each movie, sort the results in descending order of the comment count, and limit the output to 10 documents.


##### Given a year find the total number of comments created each month in that year
used Aggregation pipeline is used to project the year and month fields from the date field, match only the documents that correspond to the given year, group the documents by month and count the number of comments made each month, project only the month and count fields while excluding the _id field, and finally sort the results by month in ascending order.

##### Find top `N` movies with the highest IMDB rating
used the pipeline which consists of four stages The first stage filters out movies that do not have an IMDB rating.The second stage sorts the remaining movies by IMDB rating in descending order.The third stage limits the output to N movies.
2. The fourth stage projects only the movie title and IMDB rating, and excludes the "_id" field.


##### Find top `N` movies with the highest IMDB rating in given year
Similar to above question added year validation parameter to above query.

#### Find top `N` movies with highest IMDB rating with number of votes > 1000
Similar to above question in match add condition to check imbd.votes is greater than 1000 or not using "$gte" operator.

#### Find top 'N' movies with title matching a given pattern sorted by highest tomatoes ratings
In this question pattern is matched using regex "$regex" operator rest of the pipeline is same.

#### Find top `N` directors who created the maximum number of movies
used aggregation pipeline the first stage unwinds the "directors" array field in each movie document to create a separate document for each director.The second stage groups the movie documents by director and calculates the count of movies per director using the "$sum" accumulator.The third stage sorts the directors by the number of movies they have directed in descending order.The fourth stage projects only the director's name and the number of movies they have directed, and excludes the "_id" field.The fifth stage limits the output to N documents.


#### Find top `N` directors who created the maximum number of movies in a given year
Similar to above query add match {"$match":{"year":year}} stage to the pipeline for validating year.


#### Find top `N` directors who created the maximum number of movies for a given genre
Similar to above query add match {"$match":{"genres":genre}} stage to the pipeline for validating genres.

#### Find top `N` actors who starred in the maximum number of movies
used aggregation pipeline the first stage "unwinds" the "cast" array field in each movie document to create a separate document for each actor. The second stage groups the movie documents by actor and calculates the count of movies per actor using the "$sum" accumulator.The third stage sorts the actors by the number of movies they have appeared in, in descending order. The fourth stage projects only the actor's name and the number of movies they have appeared in, and excludes the "_id" field. The fifth stage limits the output to N documents.

#### Find top `N` actors who starred in the maximum number of movies in a given year
Similar to above query add match {"$match":{"year":year}} stage to the pipeline for validating year.

#### Find top `N` actors who starred in the maximum number of movies in a given genre
Similar to above query add match {"$match":{"genres":genre}} stage to the pipeline for validating genres.

#### Top 10 cities with the maximum number of theatres
used  aggregation pipeline which consists of four stages the first stage groups the theatres documents by city and calculates the count of theatres per city using the "$sum" accumulator.
2.The second stage sorts the cities by the number of theaters in descending order. The third stage projects only the city name and the count of theaters, and excludes the "_id" field. The fourth stage limits the output to 10 documents.


#### top 10 theatres nearby given coordinates
created a 2dsphere index on the "location.geo" field of the collection using the create_index method. The method then constructs a query using the find method on the theaters collection with a near operator, which returns documents that are closest to the given coordinates. The "$near" operator requires a 2dsphere index to be created on the field being queried. The query specifies the $geometry parameter with the type and coordinates fields to define the point location to search for. The projection parameter specifies which fields to include in the result set.



### praneeth's  code review

Each block of code follows a similar structure, beginning by creating a reference to the relevant MongoDB collection using the `mydb` object. The `mongoimport_cmd` variable is then assigned an array of strings containing the command to be executed, including the `mongoimport` tool, the database and collection names, the file type (JSON), and the file path. The `subprocess.Popen()` method is then used to execute the command, passing in the `mongoimport_cmd` variable as an argument.He used mongoimport tool to import JSON data into MongoDB using Python


#### question4_a_1
*finds the top 10 commenters by name.* grouped  by name and created a count for each. It then creates a sorting variable that sorts the comments in descending order based on their count. Finally, it creates a limiting variable that limits the results to the top 10. The comments.aggregate() method is then used to execute the aggregation pipeline, passing in the grouping, sorting, and limiting variables as arguments. The results are then printed to the console.
#### question4_a_2
grouped by movie_id and create a count for each. It then creates a sorting variable that sorts the comments in descending order based on their count and limits by 10. The function then uses the movies.find_one() method to query the movies collection for the title of each movie based on its movie_id to *get the top 10 movies with the most comments*
#### question4_a_3
used  $match stage to filter comments by the year entered, and grouped by month and counted the number of comments in each month, sorted in asc order to *get the total number of comments created each month in that year*
#### question4_b_1
queried movies collection using the find() method, with the filter conditions that the imdb.rating field is not equal to an empty string and the `year` field is equal to the value of year_. The projection parameter is used to only return the title, imdb.rating and year fields. The query results are sorted in descending order by imdb.rating and limited to `n` documents to *get top `n` movies with the highest IMDB rating in a given year*
#### question4_b_2
find() method, with the filter conditions year field is equal to the value of year. The query results are sorted in descending order by imdb.rating and limited to `n` documents.
#### question4_b_3
used find with the filter conditions that  imdb.votes field is greater than 1000. The query results are sorted in descending order by imdb.rating and limited to `n` documents *to get top `n` movies with the highest IMDB rating, votes more than 1000*
#### question4_b_4
used find() method, with the filter conditions that the tomatoes.viewer.rating field is not equal to an empty string and the title field matches the regular expression pattern specified by regex_ ,sorted in desc order and limited to n docs *to get title matching a given pattern sorted by highest tomatoes ratings*
#### question4_b_5
used `$unwind` stage which splits the directors field into multiple documents, one for each director and  grouped by  directors and calculates the count of movies created by each director using the `$sum` operator, sorts them in desc and limits to n docs to get *top `n` directors who created the maximum number of movies*
#### question4_b_6
used `$match` with year and similar process as above to *get top `n` directors who created the maximum number of movies in a given year*
#### question4_b_7
used `$match` with genre, unwind by directors,groups by director and counts the no of movies ,sorts in desc and limits to n docs to get *top `n` directors who created the maximum number of movies in a given genre*
#### question4_b_8
used unwind, by cast ,groups by cast and counts the num of movies, sorts in desc and limits to n docs to get *top `n` actors who starred in the maximum number of movies*
#### question4_b_9
matched with year  and similar process as above to get *top `n` actors who starred in the maximum number of movies in a given year*
#### question4_b_10
matched with genre and similar process as above to get *top `n` actors who starred in the maximum number of movies in a given genre*
#### question4_b_11
matched with imdb rating ,unwind by genre ,sort by imdb in desc ,groups the movies by genre, and creates an array of the top `n` movie titles for each genre. Finally, it sorts the genres by `_id` to get *top `n` movies for each genre with the highest IMDB rating*
#### question5_a
grouped the theatres by city and count the number of theatres in each city, sort the cities by count in descending order, and return only the top 10 cities with the most theatres to get *Top 10 cities with the maximum number of theatres*
#### question5_b
created a 2dsphere index on the `location.geo` field of the collection to enable the `$near` operator to efficiently find documents with geo-coordinates near the given longitude and latitude. It then queries the collection using `find()` and the `$near` operator to find the 10 documents nearest to the given coordinates, and then uses projection to include only the `theaterId` and `location.geo` fields in the result set to get *top 10 theatres nearby given coordinates*
