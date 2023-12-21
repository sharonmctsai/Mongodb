
  show dbs
  use ca2

  db.movies.find({genres: "Short"}).count()
//Part 1,Read(Find)
//Query 1 - This query shows movies with the genre "Short" and IMDb rating greater than 6.0.

  db.movies.find({
    genres: "Short",
    "imdb.rating": { $gt: 6.0 }
  }, { _id: 0, title: 1, genres: 1, "imdb.rating": 1 }).pretty();
  
//Query 2 - This query shows movies released after the year 2000 with an IMDb rating greater than or equal to 8.
db.movies.find({
    year: { $gt: 2000 },
    "imdb.rating": { $gte: 8 }
  }, { _id: 0, title: 1, year: 1, "imdb.rating": 1 }).sort({ "imdb.rating": -1 }).limit(5).pretty();
  
  
//Query 3 - This query shows movies directed by "William K.L. Dickson" that have won awards.
  db.movies.find({
    directors: "William K.L. Dickson",
    awards: { $exists: true }
  }, { _id: 0, title: 1, directors: 1, awards: 1 }).pretty();

//Query 4 - This query shows movies with viewer ratings on Tomatoes.

  db.movies.find({
    "tomatoes.viewer.rating": { $exists: true }
  }, { _id: 0, title: 1, "tomatoes.viewer.rating": 1 }).sort({ "imdb.rating": 1 }).skip(3).pretty();
  
//Query 5 - This query counts movies with more than one comment.
  db.movies.find({
    num_mflix_comments: { $gt: 1 }
  }, { _id: 0, title: 1, num_mflix_comments: 1 }).count();
  
//Query 6- This query shows movies with the cast members "Charles Kayser" and "John Ott."

  db.movies.find({
    cast: { $all: ["Charles Kayser", "John Ott"] }
  }, { _id: 0, title: 1, cast: 1 }).pretty();
  

//Part 2,Create (Insert)
//Insert 1 - This to create two movies were released between 2017 and 2021

  db.movies.insertOne({
    _id: 1,
    title: "Boiling Point",
    year: 2021,
    runtime: 120,
    cast: ["Stephen Graham", "Vinette Robinson"],
    plot: "Relentless pressure in a restaurant kitchen as a head chef wrangles his team on the busiest day of the year.",
    genres: ["Drama", "Thriller"],
    imdb: {
      rating: 7.5,
      votes: 500000
    }
  });

  //Insert 2

  db.movies.insertOne({
    _id: 2,
    title: "Us",
    year: 2019,
    runtime: 116,
    cast: ["Lupita Nyong'o", "Winston Duke", "Shahadi Wright Joseph", "Evan Alex"],
    plot: "A family's serene beach vacation turns to chaos when their doppelgängers appear and begin to terrorize them.",
    genres: ["Horror", "Thriller"],
    imdb: {
      rating: 6.9,
      votes: 450000
    }
  });
  
//Create  - three user documents.
  db.users.insertMany([
    {
      _id: 1,
      favourites: [1, 2],
      name: "Apple Green",
      email: "applegreen@wit.com",
      password: "secret"
    },
    {
      _id: 2,
      favourites: [1],
      name: "Billy Pan",
      email: "billypan@wit.com",
      password: "secret"
    },
    {
      _id: 3,
      favourites: [2],
      name: "Circle King",
      email: "circleking@wit.com",
      password: "secret"
    }
  ]);
  

//Part 3: Update, delete

//Update 1 - update the IMDB rating and votes for a movie:
  db.movies.updateOne(
    { _id: 1 }, // Choose the appropriate movie document based on its _id
    {
      $set: {
        "imdb.rating": 8.5,
        "imdb.votes": 500001
      }
    }
  );
  
//Update 2 - Add a new favorite to a user document:

  db.users.updateOne(
    { _id: 2 },
    {
      $push: {
        favourites: 2
      }
    }
  );
  
// Update 3 - Set the new runtime value (in minutes)
  db.movies.updateOne(
    { _id: 2 }, 
    {
      $set: {
        runtime: 120 
      }
    }
  );
  
// Update 4 - Set the new email address
  db.users.updateOne(
    { _id: 2 }, 
    {
      $set: {
        email: "billypan@setu.com" 
      }
    }
  );

//Delete one of the movies:
  
  db.movies.deleteOne(
    { _id: 2 }
  );
  
//Part 4: Aggregation
//Aggregation Pipeline 1 - to calculate the average IMDb rating for movies released in each year:
  db.movies.aggregate([
    {
      $match: {
        genres: "Horror" // Match documents with the "Horror" genre
      }
    },
    {
      $group: {
        _id: "$year", // Group by the year field
        averageRating: { $avg: "$imdb.rating" }, // Calculate the average IMDb rating for each group
        count: { $sum: 1 } // Count the number of documents in each group
      }
    },
    {
      $project: {
        _id: 0, // Exclude the _id field from the final output
        year: "$_id", // Rename the _id field to "year"
        averageRating: 1, // Include the averageRating field
        count: 1 // Include the count field
      }
    }
  ])
  

//Aggregation Pipeline 2 -for Users and Favourites:
  db.users.aggregate([
    { $unwind: "$favourites" }, // Unwind the favourites array
    { $lookup: { from: "movies", localField: "favourites", foreignField: "_id", as: "movieInfo" } }, // Lookup movie details
    { $project: { _id: 1, name: 1, email: 1, favourites: 1, movieInfo: { $arrayElemAt: ["$movieInfo", 0] } } }, // Project relevant fields
    { $sort: { name: 1 } }, // Sort users by name
    { $skip: 1 }, // Skip the first user
    { $limit: 2 } // Limit the results to 2 users
  ]);
  
  