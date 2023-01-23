{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Importing the modules\n",
    "import sys\n",
    "import os\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import *\n",
    "from pyspark.sql.functions import from_json"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Creating spark session\n",
    "spark = SparkSession.builder \\\n",
    "    .master(\"local\") \\\n",
    "    .appName(\"Kafka To HDFS\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "spark.sparkContext.setLogLevel('ERROR')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Creating Dataframe from Kafka Data\n",
    "df = spark \\\n",
    "    .readStream \\\n",
    "    .format(\"kafka\") \\\n",
    "    .option(\"kafka.bootstrap.servers\", \"18.211.252.152:9092\") \\\n",
    "    .option(\"subscribe\", \"de-capstone3\") \\\n",
    "    .option(\"startingOffsets\", \"earliest\") \\\n",
    "    .load()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Transforming dataframe by changing value columns' data type and dropping some columns\n",
    "\n",
    "df = df \\\n",
    "    .withColumn('value_str', df['value'].cast('string').alias('key_str')).drop('value') \\\n",
    "    .drop('key','topic','partition','offset','timestamp','timestampType')\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Writing the dataframe to local file directory and keep it running until termination\n",
    "df.writeStream \\\n",
    "    .outputMode(\"append\") \\\n",
    "    .format(\"json\") \\\n",
    "    .option(\"truncate\",\"false\") \\\n",
    "    .option(\"path\", \"clickstream_data\") \\\n",
    "    .option(\"checkpointLocation\",\"clickstream_checkpoint\") \\\n",
    "    .start() \\\n",
    "    .awaitTermination()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
