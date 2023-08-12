from pyspark import SparkContext


bson_file = open('products.bson', 'rb')
with open('products.bson', 'br') as file:
    # data = bson.decode_all(f.read())
    for ind in range(0, 1):
        line = file.readline()
        print(line)
# df = pandas.read_json(bson_file, lines = True, chunksize = 1000)
# for ind in df:
#    print(ind)
# bson_data = bson.loads(bson_file.read())
# print(df)     
