from pyspark.sql import SparkSession
from pyspark.sql.functions import when , col , ntile, sum  
from pyspark.sql.window import Window

class customer_churn: 

    def __init__(self, path): 
        self.path = path 
        self.spark =  SparkSession.builder.appName('Data_project').getOrCreate()

    def load_data(self): 

        self.df = self.spark.read.csv(
            self.path,
            header = True,
            inferSchema  = True
        )
        self.df.show() 
        return self.df 

    def make_feateus(self,doing_type = 'spark'): 

        if doing_type.lower() == 'spark': 

            from pyspark.sql.functions import col , when 

            self.df = self.df.withColumn('is_male', when(col('Gender') == 'Male', 1).otherwise(0))
            self.df = self.df.withColumn('is_female', when(col('Gender') == 'Female', 1).otherwise(0))

            window_temp = Window.orderBy(col('TotalSpent').desc())
            self.df  = self.df.withColumn('percentile_range', ntile(10).over(window_temp)) 


        elif  doing_type.lower() == 'sql':
            self.df.createOrReplaceTempView("customer_data")

            self.df = self.spark.sql("""
                with temp_temp as (
                    select *, 
                        case when Gender == 'Male' then 1 else 0 end as is_male, 
                        case when Gender == 'Female' then 1 else 0 end as is_female,
                        ntile(10) over(order by TotalSpent desc) as percentile_range

                    from customer_data
                    ) 
                select * 
                from temp_temp 
            """)

        return self.df




