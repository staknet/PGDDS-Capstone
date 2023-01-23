{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Importing the modules\n",
    "from pyspark.sql.types import *\n",
    "from pyspark.sql.functions import *\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import from_json\n",
    "from pyspark.sql.types import TimestampType, IntegerType, FloatType, ArrayType,LongType\n",
    "from pyspark.sql import functions as func"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Initializing Spark Session\n",
    "\n",
    "spark = SparkSession  \\\n",
    "    .builder  \\\n",
    "    .appName(\"StructuredSocketRead\")  \\\n",
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
    "# Defining the schema\n",
    "\n",
    "schema_agg = StructType([\n",
    "        StructField(\"booking_id\", StringType()),\n",
    "        StructField(\"customer_id\",  LongType()),\n",
    "        StructField(\"driver_id\",  LongType()),\n",
    "        StructField(\"customer_app_version\", StringType()),\n",
    "        StructField(\"customer_phone_os_version\", StringType()),\n",
    "        StructField(\"pickup_lat\", DoubleType()),\n",
    "        StructField(\"pickup_lon\", DoubleType()),\n",
    "        StructField(\"drop_lat\", DoubleType()),\n",
    "        StructField(\"drop_lon\", DoubleType()),\n",
    "        StructField(\"pickup_timestamp\", TimestampType()),\n",
    "        StructField(\"drop_timestamp\", TimestampType()),\n",
    "        StructField(\"trip_fare\", IntegerType()),\n",
    "        StructField(\"tip_amount\", IntegerType()),\n",
    "        StructField(\"currency_code\", StringType()),\n",
    "        StructField(\"cab_color\", StringType()),\n",
    "        StructField(\"cab_registration_no\", StringType()),\n",
    "        StructField(\"customer_rating_by_driver\", IntegerType()),\n",
    "        StructField(\"rating_by_customer\", IntegerType()),\n",
    "        StructField(\"passenger_count\", IntegerType())\n",
    "        ])\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Reading the file \"part-m-00000\" in bookings_data folder in hadoop\n",
    "df=spark.read.csv(\"bookings_data/part-m-00000\", schema=schema_agg)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Creating data from column \"pickup_date\" and \"pickup_timestamp\"\n",
    "df = df.withColumn(\"pickup_date\", func.to_date(func.col(\"pickup_timestamp\")))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Group the data by \"pickup_date\"\n",
    "date = df.groupBy('pickup_date').count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Saving the datewise total booking data in .csv format\n",
    "date.coalesce(1).write.format('csv').save(\"date_aggregated_data/\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Saving the booking data in .csv format\n",
    "df.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('booking_data_csv', header = 'true')"
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
