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

    from pyspark.sql import functions as f

    def data_handling(self, outlier_method='z_score'): 
        # Drop missing values
        self.df = self.df.dropna()  

        numeric_features = ['feature1', 'feature2', 'feature3']  # Replace with actual features

        if outlier_method.lower() == 'z_score':  
            for feature in numeric_features:
                # Compute mean and standard deviation
                mean, std = self.df.select(
                    f.mean(f.col(feature)).alias('mean'),
                    f.stddev(f.col(feature)).alias('std')
                ).collect()[0]

                # Compute lower and upper boundaries
                lower_bound = mean - 3 * std
                upper_bound = mean + 3 * std

                # Filter outliers
                self.df = self.df.filter(
                    (f.col(feature) >= lower_bound) & (f.col(feature) <= upper_bound)
                )

        elif outlier_method.lower() == 'iqr':  
            for feature in numeric_features:
                # Compute Q1 and Q3
                q1, q3 = self.df.approxQuantile(feature, [0.25, 0.75], 0)

                # Compute IQR
                iqr = q3 - q1

                # Compute lower and upper boundaries
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                # Filter outliers
                self.df = self.df.filter(
                    (f.col(feature) >= lower_bound) & (f.col(feature) <= upper_bound)
                )

        self.df.show()
        return self.df
