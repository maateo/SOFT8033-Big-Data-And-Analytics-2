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


def get_key_value(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line char
    line = line.replace('\n', '')

    # 3. We get the key and value
    words = line.split('\t')
    day = words[0]
    hour = words[1]

    # 4. We process the value
    hour = hour.rstrip(')')
    hour = hour.strip('(')

    # 4. We assign res
    res = (day, hour)

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    # Not sure what my_mapper_input_parameters is for.

    for input in my_input_stream:
        processed_input = process_line(input)

        project = processed_input[0]
        page_name = processed_input[1]
        language = processed_input[2]
        num_views = processed_input[3]

        # So, we want it to look something like: project_language, page_name, num_views
        # eg: Wikipedia_English, Olympic Games, 211
        string_to_write = "%s_%s, %s, %s\n" % (project, language, page_name, num_views)

        my_output_stream.write(string_to_write)

    pass


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    outputs_dictionary = {}

    for input in my_input_stream:
        p_p_n = input.split(",")  # gives ["Wikipedia_English","Olympic Games", "211"]

        project_language = p_p_n[0]  # "Wikipedia_English"
        page_name = p_p_n[1]  # "Olympic Games"
        num_views = p_p_n[2]  # "211"

        if project_language in outputs_dictionary:
            # We have the project_language key in there
            if int(num_views) > int(outputs_dictionary[project_language][1]):
                # If our current num_views is grater  than the number of views that the previous entry had
                outputs_dictionary[project_language] = (page_name, num_views)  # Replace the value
        else:
            # We don't have that key, so create and initialise it with the data
            outputs_dictionary[project_language] = (page_name, num_views)

    for output in outputs_dictionary.keys():
        string_to_write = "%s\t(%s, %s)" % (output, outputs_dictionary[output], outputs_dictionary[output])
        my_output_stream.write(string_to_write)

    pass


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


# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
    print("HELLO!")
    inputDStream = ssc.textFileStream(monitoring_dir)

    mappedDStream = inputDStream.map(process_line)  # Gives (project, web-page, language, views)

    # Map in the format of (project_language, page_name, num_views)
    # eg: Wikipedia_English, Olympic Games, 211
    mappedDStream = mappedDStream.map(lambda row: ("%s_%s" % (row[0], row[2]), (row[1], int(row[3]))))

    windowDStream = mappedDStream.window(60, 60)

    # Reduce it so the highest of each key is kept
    reducedRDD = windowDStream.transform(lambda rdd: rdd.reduceByKey(lambda x, y: x if (x[1] > y[1]) else y))

    # Sort it so highest num views if first
    sortedRDD = reducedRDD.transform(lambda rdd: rdd.sortBy(lambda row: row[1][1], False))

    sortedRDD.pprint()

    pass


if __name__ == '__main__':
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    my_dataset_dir = "FileStore/tables/3_Assignment/matfiles/"
    my_spark_core_model(sc, my_dataset_dir)
