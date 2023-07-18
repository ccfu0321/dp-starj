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

# extract the distinct values of the predicate attribute
query1 = "select distinct p_category from part ;"
list1 = result(query1)
query2 = "select distinct s_region from supplier ;"
list2 = result(query2)

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
    p = list1.index(where_strings[where_strings.index('p_category') + 2])
    s = list2.index(where_strings[where_strings.index('s_region') + 2])
    # add the noise to the predicate
    re_cate = list1[index_noise(p, len(list1), epsilon / dim_num)]
    re_region = index_noise(s, len(list2), epsilon / dim_num)
    # noisy star-join query
    where_strings[where_strings.index('p_category') + 2] = re_cate
    where_strings[where_strings.index('s_region') + 2] = list2[re_region]
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



