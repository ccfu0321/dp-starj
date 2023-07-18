import psycopg2
import numpy as np


def result(sql_query):
    con = psycopg2.connect(database='database_name')
    cur = con.cursor()
    # run the query and get the result
    cur.execute(sql_query)
    res = cur.fetchall()
    con.commit()
    con.close()
    return res

# extract the distinct values of the predicate attribute
query = "select distinct table_attribute from table ;"
list = result(query)

# generate exponentially distribution
weights = np.random.exponential(size=len(list))
weights /= weights.sum()
data = np.random.choice(list, size=table_size, p=weights)
data = data.tolist()


# uniform distribution
# data = [random.choice(list) for _ in range(table_size)]

# gamma distribution
# alpha = [2, 4, 6, 8, 10, 2, 4, 6, 8, 10, 2, 4, 6, 8, 10, 2, 4, 6, 8, 10, 2, 4, 6, 8, 10]
# beta = 1
# probabilities = [np.random.gamma(a, beta) for a in alpha]
# probabilities = probabilities / np.sum(probabilities)
# size = table_size
# data = np.random.choice(list, size=size, p=probabilities)
# data = data.tolist()


# mixture of Gaussian distributions
# num_samples = table_size
# #Define the mean and standard deviation of the two categories
# class1_mean, class1_stdev = 1, 2
# class2_mean, class2_stdev = 5, 2
# #generate data that follows a two-category mixture of Gaussian distributions
# data_list = []
# for _ in range(num_samples):
#     category = np.random.choice(list)
#   # distinguish between two classes
#     if category in list[1:int(len(list)/2)]:
#         sample = np.random.normal(class1_mean, class1_stdev)
#     else:
#         sample = np.random.normal(class2_mean, class2_stdev)
#     data_list.append((category, sample))
# data = []
# for item in data_list:
#     data.append(int(item[0]))

con = psycopg2.connect(database='database name')
cur = con.cursor()
# update table with generated data
for i, value in enumerate(data):
    query = "UPDATE table SET table_attribute = %s WHERE id = %s;"
    cur.execute(query, (value, i+1))

con.commit()
con.close()
