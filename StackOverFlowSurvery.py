from pyspark.sql import SparkSession

AGE_MIDPOINT = "age_midpoint"
SALARY_MIDPOINT = "salary_midpoint"
SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

if __name__ == "__main__":

    session = SparkSession.builder.appName("StackOverFlowSurvey").getOrCreate()
    sc = session.sparkContext
    sc.setLogLevel('ERROR')
    
    dataFrameReader = session.read

    responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("s3://datafileky/2016-stack-overflow-survey-responses.csv")

    print("=== Print out schema ===")
    responses.printSchema()
    
    responseWithSelectedColumns = responses.select("country", "occupation", 
        AGE_MIDPOINT, SALARY_MIDPOINT)

    print("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show()

    print("=== Print records where the response is from Afghanistan ===")
    responseWithSelectedColumns\
        .filter(responseWithSelectedColumns["country"] == "Afghanistan").show()

    print("=== Print the count of occupations ===")
    groupedData = responseWithSelectedColumns.groupBy("occupation")
    groupedData.count().show()

    print("=== Print records with average mid age less than 20 ===")
    responseWithSelectedColumns\
        .filter(responseWithSelectedColumns[AGE_MIDPOINT] < 20).show()

    print("=== Print the result by salary middle point in descending order ===")
    responseWithSelectedColumns\
        .orderBy(responseWithSelectedColumns[SALARY_MIDPOINT], ascending = False).show()

    print("=== Group by country and aggregate by average salary middle point ===")
    dataGroupByCountry = responseWithSelectedColumns.groupBy("country")
    dataGroupByCountry.avg(SALARY_MIDPOINT).show()

    responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
        ((responses[SALARY_MIDPOINT]/20000).cast("integer")*20000))

    print("=== With salary bucket column ===")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    print("=== Group by salary bucket ===")
    responseWithSalaryBucket \
        .groupBy(SALARY_MIDPOINT_BUCKET) \
        .count() \
        .orderBy(SALARY_MIDPOINT_BUCKET) \
        .show()

    
    print("=== My Works ===")

    responseWithSelectedColumns = responses.select("occupation_group", 
        "dogs_vs_cats", "job_satisfaction")

    print("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show()

    print("=== null drop test ===")
    naDf = responseWithSelectedColumns.na.drop()
    naDf.show()

    print("=== Tidy Some Data ===")
    filteredDf = naDf\
        .filter(naDf['dogs_vs_cats'].isin(['Dogs','Cats']))
    filteredDf.show()

    print("=== Print the count  ===")
    groupedData = filteredDf.groupBy("occupation_group","dogs_vs_cats","job_satisfaction").count()
    groupedData.orderBy("occupation_group","dogs_vs_cats","job_satisfaction").show()

    print("=== Dogs vs Cats related to Job Satisfaction ? ===")
    newDf = groupedData.groupBy("dogs_vs_cats", "job_satisfaction").sum("count")
    
    newDf = newDf.withColumn("floor_count", ((newDf["sum(count)"]/10).cast("integer")*10))
    newDf.orderBy("dogs_vs_cats", "job_satisfaction").show()
    

    # responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
    #     ((responses[SALARY_MIDPOINT]/20000).cast("integer")*20000))




    session.stop()