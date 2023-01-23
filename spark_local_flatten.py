{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Importing modules\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import *\n"
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
    "spark.sparkContext.setLogLevel('ERROR')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Reading json data into dataframe \n",
    "df = spark.read.json('clickstream_data/part-00000-09f8130f-4a14-890f-d28b-fec98641896b-c000.json')\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Extracting columns from json values and creating new dataframe with new columns \n",
    "df = df.select( \\\n",
    "               get_json_object(df[\"value_str\"],\"$.customer_id\").alias(\"customer_id\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.app_version\").alias(\"app_version\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.OS_version\").alias(\"OS_version\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.lat\").alias(\"lat\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.lon\").alias(\"lon\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.page_id\").alias(\"page_id\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.button_id\").alias(\"button_id\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.is_button_click\").alias(\"is_button_click\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.is_page_view\").alias(\"is_page_view\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.is_scroll_up\").alias(\"is_scroll_up\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.is_scroll_down\").alias(\"is_scroll_down\"),\\\n",
    "               get_json_object(df[\"value_str\"],\"$.timestamp\").alias(\"timestamp\"),\\\n",
    "              )\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Checking the schema\n",
    "print(df.schema)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Printing 10 records from the dataframe\n",
    "df.show(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Saving the dataframe as a csv file in local directory\n",
    "df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('user/root/clickstream_data_flatten', header = 'true')"
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
