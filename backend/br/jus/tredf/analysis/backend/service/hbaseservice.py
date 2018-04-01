from starbase import Connection

c = Connection("192.168.137.145", "8000")

ratings = c.table('ratings')
if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

ratings.create("rating")

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("C:/trabalho/hadoop/training/HadoopMaterials/ml-100k/u.data", "r")

batch = ratings.batch()

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingFile.close()

print('Commiting ratings data to HBase via REST service\n')
batch.commit(finalize=True)

print('get back ratings for some users...\n')
print('Ratings for user id 1:\n')
print(ratings.fetch(1))
