import psycopg2
import numpy as np
import time


def result(sql_query):
    con = psycopg2.connect(database='database_name')
    cur = con.cursor()
    # Run the query and get the results
    cur.execute(sql_query)
    res = cur.fetchall()
    con.commit()
    con.close()
    return res


query1 = "select distinct c_region from customer ;"
list1 = result(query1)
query2 = "select distinct s_nation from supplier ;"
list2 = result(query2)
query3 = "select distinct p_mfgr from part ;"
list3 = result(query3)
query4 = "select distinct d_year from dates ;"
list4 = result(query4)

query = ""
query_file = open('query_path', 'r')

# Read the query file and store in query
for line in query_file.readlines():
    query = query + line
    if ";" in query:
        query = query.replace('\n', " ")
        break


def laplace_mech(v, sensitivity, epsilon):
    return v + np.random.laplace(loc=0, scale=sensitivity/epsilon)


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


def index_noise(value, domain, epsilon):
    counter = 1
    while counter > 0:
        x = int(laplace_mech(value, domain, epsilon))
        if x in range(domain):
            noise_value = x
            break

    return noise_value


def predicate_mechanism(query, epsilon):
    where_string = ""
    words = query.split(" ")
    for word in words:
        if word.lower() == "where":
            where_string = word

    parser_string = query
    parser_string = parser_string.replace(where_string + " ", "\n")
    parser_string = parser_string.replace(";", " ")
    parser_strings = parser_string.split("\n")
    where_strings = parser_strings[1].split(" ")

    # extract the predicate of star-join query
    a = list1.index(where_strings[where_strings.index('c_region') + 2])
    b = list2.index(where_strings[where_strings.index('s_nation') + 2])
    s_l = list3.index(where_strings[where_strings.index('or') - 1])
    s_r = list3.index(where_strings[where_strings.index('or') + 3])
    y_l = list4.index(where_strings[where_strings.index('between') + 1])
    y_r = list4.index(where_strings[where_strings.index('between') + 3])
    # add the noise to the predicate
    index1 = index_noise(a, len(list1), epsilon / dim_num)
    index2 = index_noise(b, len(list2), epsilon / dim_num)
    re_cate1 = str(list3[index_noise(int(s_l[1]), len(list3), epsilon / dim_num)])
    re_cate2 = str(list3[index_noise(int(s_r[1]), len(list3), epsilon / dim_num)])
    re_y = range_noise(y_l, y_r, list4, epsilon / dim_num)
    # noisy star-join query
    where_strings[where_strings.index('c_region') + 2] = list1[index1]
    where_strings[where_strings.index('s_nation') + 2] = list2[index2]
    where_strings[where_strings.index('or') - 1] = re_cate1
    where_strings[where_strings.index('or') + 3] = re_cate2
    where_strings[where_strings.index('between') + 2] = re_y[0]
    where_strings[where_strings.index('between') + 3] = re_y[1]

    parser_strings[1] = ' '.join(where_strings)
    re_query = parser_strings[0] + 'where' + ' ' + parser_strings[1] + ';'

    return re_query


def relative_error(y_true, y_pred):
    error = (y_true - y_pred)/y_true
    return error


def avg(error_list):
    sum = 0
    for item in error_list:
        sum += item
    return (sum/len(error_list))


def main():
    # the true result of the star-join query
    true_res = result(query)
    # it is better to set it as a separate value
    epsilons = [0.1, 0.2, 0.5, 0.8, 1.0]
    error_list = []
    time_list = []
    output = []
    output_time = []
    for epsilon in epsilons:
        for i in range(repeat_times):
            startTime = time.time()
            # the noise query of the star-join query by predicate mechanism
            noise_query = predicate_mechanism(query, epsilon)
            noise_res = result(noise_query)
            # the noise result of the star-join query
            noise = noise_res[0][0]
            endTime = time.time()
            error_list.append(relative_error(true_res, noise))
            time_list.append(endTime - startTime)
        spendTime = avg(time_list)
        # convert to percentage
        error_avg = avg(error_list) * 100
        output.append(str(epsilon) + "/" + str(error_avg))
        output_time.append(str(epsilon) + "/" + str(spendTime))
    return (output, output_time)


if __name__ == "__main__":
    main()



