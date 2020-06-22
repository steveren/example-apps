import pymongo

# replace with your Atlas connection string
client = pymongo.MongoClient(
      "mongodb+srv://<username>:<password>@<cluster-name>/test?retryWrites=true&w=majority")
db = client.test
 
def basic_crud():

  # use a collection named "fruit"
  my_collection = db["fruit"]
  my_collection.drop()

  # build a document
  my_document = { "type": "apple", "quantity": 10 }

  # insert the document
  my_collection.insert_one(my_document)
  
  # add a few more documents
  my_documents = [{ "type": "orange", "quantity": 30 }, 
                  { "type": "banana", "quantity": 15 },
                  { "type": "pear", "quantity": 20 } ]
  
  my_collection.insert_many(my_documents)
  
  # read them back
  print("Documents in collection 'fruit': ")
  for doc in my_collection.find():
    print(doc)

def more_crud():

  # use a collection named "cars"
  my_collection = db["vehicles"]
  my_collection.drop()

  # insert some documents
  my_documents = [ {"type": "car", "specs": { "wheels": 4, "doors": 2 }, "color": "green" },
                   {"type": "motorcycle", "specs": { "wheels": 2, "doors": 0 }, "color": "yellow" },
                   {"type": "airplane", "specs": { "wheels": 3, "doors": 1 }, "color": "blue" } ]
  
  my_collection.insert_many(my_documents)

  # read out the collection
  print("All vehicles:")
  for doc in my_collection.find():
    print(doc)

  # find vehicles with at least 3 wheels
  print("Vehicles with at least 3 wheels:")
  for doc in my_collection.find({ "specs.wheels": { "$gte": 3 } }):
    print(doc)

  # sort the vehicles
  print("Vehicles sorted by number of doors:")
  for doc in my_collection.find().sort("specs.doors"):
    print(doc)

  # update the car's color to red
  my_collection.update_many({ "type": "car"}, { "$set": { "color": "red" } })

  # delete any vehicles with 0 doors
  my_collection.delete_many({ "specs.doors": 0 })

  # show all remaining documents, but don't display the _id field
  print("Remaining vehicles:")
  for doc in my_collection.find({}, { "_id": 0 }):
    print(doc)

def aggregation():
  
  # use a collection named "inventory"
  my_collection = db["inventory"]
  my_collection.drop()

  # insert some documents
  my_documents = [{ "type": "pens", "qty": 300, "cost_per_unit": 1.29 },
                  { "type": "pencils", "qty": 500, "cost_per_unit": 0.19 },
                  { "type": "calculators", "qty": 600, "cost_per_unit": 12.89 },
                  { "type": "toner_cartridges", "qty": 250, "cost_per_unit": 26.50 }]

  my_collection.insert_many(my_documents)

  # find a total
  print("Total units in stock:")
  pipeline = [{ "$group": { "_id": None, "totalQty": { "$sum": "$qty" } } },
              { "$project": { "_id": 0 } }]
  for doc in my_collection.aggregate(pipeline):
    print(doc)

  # find an average
  print("Average cost of pens and pencils:")
  pipeline = [{ "$match": { "$or": [{ "type": "pens" }, { "type": "pencils" }] } },
              { "$group": { "_id": None, "averageCost": { "$avg": "$cost_per_unit" } } },
              { "$project": { "_id": 0 } }]
  for doc in my_collection.aggregate(pipeline):
    print(doc)

def administration():

  # use a collection called "dogs"
  my_collection = db["dogs"]
  my_collection.drop()

  # add some documents
  my_documents = [{ "breed": "Rottweiler", "size": "extra large", "colors": ["black", "brown", "tan"] },
                  { "breed": "Irish Setter", "size": "large", "colors": ["red", "brown"] },
                  { "breed": "Cocker Spaniel", "size": "medium", "colors": ["white", "brown", "black"] }]

  my_collection.insert_many(my_documents)   

  # add a single key unique ascending index on the "breed" field
  my_collection.create_index("breed", unique=True)

  # add a compound index on the "breed" and "size" fields
  my_collection.create_index([("breed", pymongo.DESCENDING), ("size", pymongo.ASCENDING)])

  # list existing indexes
  print("Indexes for test.dogs:")
  print(my_collection.index_information())

  # drop the unique index
  my_collection.drop_index("breed_1")

  # rename the collection to "canines"
  my_collection.rename("canines", dropTarget=True)

  # list the collections in this database
  print("Collections in the 'test' database:")
  print(db.list_collection_names())
   
def main():
  
  basic_crud()
  # more_crud()
  # aggregation()
  # administration()

if __name__ == '__main__':
    main()