import bson
from pyspark.sql import SparkSession
from pyspark.sql import Row
def write_data(spark):

    #cluster = Cluster(['cassandra'])
    #session = cluster.connect('test')
    #with open('crop.bson', 'rb') as f:
    #    data = bson.loads(f.read())
    #    print(data)
    #    print('---------------------')
    result = []
    with open('crop.bson', 'rb') as f:
        content = f.read()
        base = 0
        while base < len(content):
            try:
                base, d = bson.decode_document(content, base)
                result.append(d)
            except:
                break

    #num_json = defaultdict(int)
    #len_json = len(result)
    '''
    for obj in result:
        for attribute, value in obj.items():
            if isinstance(value, int) or isinstance(value, str):
                num_json[attribute] = num_json[attribute] + 1
                #print(attribute, value)  # example usage
    list_key = []
    for attribute, value in num_json.items():
        if value == len_json:
            list_key.append(attribute)
    
    for obj in result:
        for attribute, value in obj.items():
            if attribute in list_key:
                print(attribute, value)
    '''
    #session.execute("truncate test.data") #dsfasdfasfasfasfasfsaaf
    list_attr = ["id", "popularity_key", "complete", "nutrition_score_beverage",
                 "pnns_groups_2", "lc", "created_t", "countries", "last_modified_t", "pnns_groups_1", "creator",
                 "nutrition_data_per", "lang", "rev", "nutrition_data_prepared_per", "update_key"]
    table_name = 'test.data'
    query = "INSERT INTO test.data (id, popularity_key, complete, nutrition_score_beverage,\
                pnns_groups_2, lc, created_t, countries, last_modified_t, pnns_groups_1, creator,\
                 nutrition_data_per, lang, rev, nutrition_data_prepared_per, update_key) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, %s, %s)"
    list_row = []
    for obj in result:
        temp_dict = {}
        for attribute, value in obj.items():
            if attribute in list_attr:
                temp_dict[attribute] = value
        row = (str(temp_dict["id"]), str(temp_dict["popularity_key"]), \
               int(temp_dict["complete"]), int(temp_dict["nutrition_score_beverage"]), \
               str(temp_dict["pnns_groups_2"]), str(temp_dict["lc"]), str(temp_dict["created_t"]),
                str(temp_dict["countries"]), str(temp_dict["last_modified_t"]),
               str(temp_dict["pnns_groups_1"]), str(temp_dict["creator"]), \
               str(temp_dict["nutrition_data_per"]), str(temp_dict["lang"]), int(temp_dict["rev"]),
               str(temp_dict["nutrition_data_prepared_per"]), str(temp_dict["update_key"]))
        #print(row)
        list_row.append(row)
        #session.execute(query, row) #asdfasasfasfasfasfasfasfasfasfasf

    df = spark.createDataFrame(data=list_row, schema=["id", "popularity_key", "complete", "nutrition_score_beverage",
                 "pnns_groups_2", "lc", "created_t", "countries", "last_modified_t", "pnns_groups_1", "creator",
                 "nutrition_data_per", "lang", "rev", "nutrition_data_prepared_per", "update_key"])
    df.show()
    query = df.write.format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "test") \
        .option("table", "data") \
        .option("confirm.truncate", "true") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    #print(temp_dict)

    #query = "INSERT INTO test.data JSON {0}".format(str(temp_dict))
    #test = {'some_1': 0, 'some_2': 1}

    #print(query)
    #row = (str(temp_dict["id"]), int(temp_dict["popularity_key"]), \
    #    int(temp_dict["complete"]), int(temp_dict["nutrition_score_beverage"]),\
    #    str(temp_dict["pnns_groups_2"]), str(temp_dict["lc"]), str(temp_dict["created_t"]), str(temp_dict["ecoscore_grade"]), str(temp_dict["countries"]), str(temp_dict["last_modified_t"]), str(temp_dict["pnns_groups_1"]), str(temp_dict["creator"]),\
    #    str(temp_dict["nutrition_data_per"]), str(temp_dict["lang"]), int(temp_dict["rev"]), str(temp_dict["nutrition_data_prepared_per"]), str(temp_dict["update_key"]))
    #print(row)
    #session.execute(query, row)
