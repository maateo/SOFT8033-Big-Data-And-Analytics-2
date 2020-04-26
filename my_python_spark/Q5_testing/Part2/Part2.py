import pyspark


def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 4):
        res = tuple(params)

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):
    inputRDD = sc.textFile(my_dataset_dir)

    mappedRDD = inputRDD.map(process_line)  # Gives (project, web-page, language, views)

    # Map in the format of (project_language, page_name, num_views)
    # eg: Wikipedia_English, Olympic Games, 211
    mappedRDD = mappedRDD.map(lambda row: ("%s_%s" % (row[0], row[2]), (row[1], int(row[3]))))

    # Reduce it so the highest of each key is kept
    reducedRDD = mappedRDD.reduceByKey(lambda x, y: x if (x[1] > y[1]) else y)

    # Sort it so highest num views if first
    sortedRDD = reducedRDD.sortBy(lambda row: row[1][1] * (-1))
    # sortedRDD = reducedRDD.sortBy(lambda a: a[1][1], False)

    for item in sortedRDD.collect():
        print(item)

    pass


if __name__ == '__main__':
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    my_dataset_dir = "FileStore/tables/3_Assignment/matfiles/"
    my_spark_core_model(sc, my_dataset_dir)
