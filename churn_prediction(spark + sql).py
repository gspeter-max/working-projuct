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
	
	
	"""
	if  date_coulumn in your_data: 
		from pyspark.sql.functions import datediff, current_date, col
		self.df = self.df.withcolumn('recency', datediff(current_date() ,col('last_order_date'))) 
		
		if you say code == 'sql' : 

			select 
				customer_id, 
	`			max(order_date) as last_order_date , 
				date_diff(currrent_date() , max(order_date) ) as recency 
			from customer_churn 
			groupby customer_id ; 
	
		
	ok next is frequency that have some way to do that : 
	1. 	from pyspark.sql.functions import count 

		df_freq = self.df.groupby('customer_id').agg(
			count('order_id').alias('freq') 
			) 
		self.df = self.df.join( df_freq , on = 'customer_id ', how = left) 
		
		$$$ that is because that make another dataframe not create column $$$ 
			
		from pyspark.sql.window import Window
		self.df = self.df.withColumn( 'freq ' , count('order_id').over( Window.partitionBy('customer_id')) 
		
		if yousay == 'sql' : 
			
			select 
				customer_id ,
				count(order_id) as freq 
			from customer_churn
			group by customer_id ; 
		
		
		*( total amount spend ) 

		self.df = self. df('total_amount_spending', f.sum('amount').over(Window.partitionBy('customer_id'))) 
		if yousay == 'sql' : 
			
			select 
				customer_id , 
				sum(amount) as total_amount_spending 
			from customer_churn 
			group by customer_id 
			    
	"""	
	def make_ml_features(categorical_features, method = 'sql') : 
		if method.lower()  == 'spark' :  
			from pyspark.ml.features import OneHotEncoder , StringIndexer
			from pyspark.ml import Pipline 
		
			if isinstance(categorical_features , str) : 
				categorical_features = [categorical_features] 
		
			indexers = [ StringIndexer(inputCol = col , outputCol = f'{col}_index' ) for col in categorical_features ] 
			encoders = [ OneHotEncoder( inputCol = f'{col}_index' , outputCol = f'{col}_encoder') for col in categorical_features ] 
		
			pipline = Pipline(stages =  indexers + encoders ) 
		
			self.df = pipline.fit(self.df).transform(self.df) 
			self.df.show() 	
			return self.df

		elif method.lower() == 'sql': 
		
			self.df.createOrReplaceTempView('customer_churn') 
			
			spark.sql(
				'''
					select * 
					from  ( 
						select product from customer_churn 
						) as sorce_function 
					pivot ( 
						count( products), 
						for product in ( 'apple','banana') 
						) as pivot_table 

 

					
 


