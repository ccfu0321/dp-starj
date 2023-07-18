import numpy as np
import psycopg2
import time


def result1(sql_query):
    con = psycopg2.connect(database='database name')
    cur = con.cursor()
    cur.execute(sql_query)
    res = cur.fetchall()
    con.commit()
    con.close()
    return res[0][0]

# extract the distinct values of the predicate attribute
query1 = "select distinct d_year from dates ;"
list1 = result1(query1)
query2 = "select distinct s_region from supplier ;"
list2 = result1(query2)
query3 = "select distinct c_region from customer ;"
list3 = result1(query3)

# workload_query = [
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1992 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1993 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1994 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1995 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1996 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1997 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1998 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1994 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1995 and 1997 ;",
# "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1997 ;",
# # "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region = 'AMERICA' and d_year between 1992 and 1994 ;",
# # "select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region = 'AMERICA' and d_year between 1995 and 1998 ;"
#  ]

workload_query = [
"select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year = 1992 ;",
"select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1993 ;",
"select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1994 ;",
"select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1995 ;",
"select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1996 ;",
"select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1997 ;",
"select count(*) from customer, lineorder, supplier, dates where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year between 1992 and 1998 ;"
]


def laplace_mech(v, sensitivity, epsilon):
    return v + np.random.laplace(loc=0, scale=sensitivity/epsilon)


def index_noise(value, epsilon, list):
    counter = 1
    while counter > 0:
        x = int(laplace_mech(value, len(list), epsilon))
        if x in range(len(list)):
            noise_value = x
            break

    return list[noise_value]


def range_noise(l_value, r_value, list, epsilon):
    counts = 1
    noise_list = []
    while counts > 0:
        x = int(laplace_mech(l_value, len(list), epsilon/2))
        y = int(laplace_mech(r_value, len(list), epsilon/2))
        if (x in list) and (y in list) and (x < y) and (y - x == r_value - l_value):
            noise_list.append([x, y])
            break
    return noise_list


def result(sql_query):
    con = psycopg2.connect(database='database name')
    cur = con.cursor()
    cur.execute(sql_query)
    res = cur.fetchall()
    con.commit()
    con.close()
    return res[0][0]


def trueAns(workload):
    true_result = []
    for i in range(len(workload)):
        true_result.append(result(str(workload[i])))
    return true_result


def noiseAns(workload):
    noisy_result = []
    for j in range(len(workload)):
        noisy_result.append(result(str(workload[j])))
    return noisy_result


def PredMech(workload_query, epsilon):
    re_query = []
    for i in range(len(workload_query)):
        if i < 1:
        # if i < 7:
            query1 = str(workload_query[i])
            words = query1.split(" ")
            # extract the predicate of query
            a = list2.index(words[words.index('s_region') + 2])
            a1 = list3.index(words[words.index('c_region') + 2])
            # add the noise to the predicate
            noisy_a = index_noise(a, epsilon/3, list2)
            noisy_a1 = index_noise(a1,  epsilon / 3, list3)
            noise_year = index_noise(int(words[words.index('d_year') + 2]), epsilon / 3, list1)
            # noisy queries
            words[words.index('s_region') + 2] = noisy_a
            words[words.index('c_region') + 2] = noisy_a1
            words[words.index('d_year') + 2] = str(noise_year)
            re_query.append(' '.join(words))
        else:
            query2 = str(workload_query[i])
            words2 = query2.split(" ")
            b = list2.index(words2[words2.index('s_region') + 2])
            b1 = list3.index(words2[words2.index('c_region') + 2])
            noisy_b = index_noise(b, epsilon / 3, list2)
            noisy_b1 = index_noise(b1, epsilon / 3, list3)
            words2[words2.index('s_region') + 2] = noisy_b
            words2[words2.index('c_region') + 2] = noisy_b1
            s_l = int(words2[words2.index('between') + 1])
            s_r = int(words2[words2.index('between') + 3])
            f = range_noise(int(s_l), int(s_r), list1, epsilon/3)
            words2[words2.index('between') + 1] = str(f[0][0])
            words2[words2.index('between') + 3] = str(f[0][1])
            re_query.append(' '.join(words2))
    return re_query



def DecomPred(workload, epsilon):
    query = str(workload[0])
    words = query.split(" ")
    re_query = []
    noise_date = []
    for i in list1:
        noise_date.append(index_noise(i, epsilon/3, list1))
    a = list2.index("'ASIA'")
    a1 = list3.index("'ASIA'")
    noisy_a = index_noise(a, epsilon/3, list2)
    noisy_a1 = index_noise(a1, epsilon / 3, list3)
    words[words.index('s_region') + 2] = noisy_a
    words[words.index('c_region') + 2] = noisy_a1
    for j in range(len(noise_date)):
        words[words.index('d_year') + 2] = str(noise_date[j])
        re_query.append(' '.join(words))
    return re_query


def relative_error(y_true, y_pred):
    error = (y_true - y_pred)/y_true
    return error


def avg(error_list):
    sum = 0
    for item in error_list:
        sum += item
    return (sum/len(error_list))


def list_error(true_list, pred_list):
    error_list = []
    for i in range(len(true_list)):
        error_list.append(relative_error(true_list[i], pred_list[i]))
    return avg(error_list)


def cdf(list1):
    list2 = []
    for i in range(len(list1)):
        list2.append(sum(list1[:i+1]))
    return list2


def main():
    # it is better to set it as a separate value
    epsilons = [0.1,0.2,0.5,0.8,1]
    # the true result of the star-join workload query
    true_result = trueAns(workload_query)
    error_list1 = []
    error_list2 = []
    output1 = []
    output2 = []
    for epsilon in epsilons:
        for i in range(repeat_times):
            # use the predicate mechanism directly
            pm_query = PredMech(workload_query, epsilon)
            noise_pm = noiseAns(pm_query)
            # use the workload decomposition
            dp_query = DecomPred(workload_query, epsilon)
            noise_dp = noiseAns(dp_query)
            noise_d = cdf(noise_dp)
            error_list1.append(list_error(true_result, noise_pm))
            error_list2.append(list_error(true_result, noise_d))
        error1 = avg(error_list1) * 100
        error2 = avg(error_list2) * 100
        output1.append(str(epsilon) + "/" + str(error1))
        output2.append(str(epsilon) + "/" + str(error2))

    return (output1, output2)


if __name__ == "__main__":
    main()



