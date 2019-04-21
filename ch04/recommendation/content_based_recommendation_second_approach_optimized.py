MATCH (movie:Movie)
WHERE movie.movieId = "187031"
MATCH (movie)-[r:DIRECTED|:HAS_GENRE]-(feature:Feature)
with id(feature) as featureId
order by featureId
RETURN collect(featureId);