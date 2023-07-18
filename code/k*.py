import psycopg2
import numpy as np
import time

query = ""
query_file = open("query path", 'r')


for line in query_file.readlines():
    query = query + line
    if ";" in query:
        query = query.replace('\n', " ")
        break


def laplace_mech(v, sensitivity, epsilon):
    return v + np.random.laplace(loc=0, scale=sensitivity/epsilon)


def range_noise(l_value, r_value, domain, epsilon):
    counts = 1
    noise_list = []
    while counts > 0:
        x = int(laplace_mech(l_value, domain, epsilon/2))
        y = int(laplace_mech(r_value, domain, epsilon/2))
        if (x in range(0, domain)) and (y in range(0, domain)) and (x < y):
            noise_list.append([x, y])
            break
    return noise_list


def rewrite_query(query, epsilon):
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

    # extract the predicate of query
    b_l = where_strings[where_strings.index('between') + 1]
    b_r = where_strings[where_strings.index('between') + 3]
    # add the noise to the predicate
    f = range_noise(int(b_l), int(b_r), node_number, epsilon)
    # noisy star-join query
    where_strings[where_strings.index('between') + 1] = str(f[0][0])
    where_strings[where_strings.index('between') + 3] = str(f[0][1])

    parser_strings[1] = ' '.join(where_strings)
    re_query = parser_strings[0] + 'where' + ' ' + parser_strings[1] + ';'

    return re_query


def result(sql_query):
    con = psycopg2.connect(database='database name')
    cur = con.cursor()
    cur.execute(sql_query)
    res = cur.fetchall()
    con.commit()
    con.close()
    return res[0][0]


def relative_error(y_true, y_pred):
    error = (y_true - y_pred)/y_true
    return error


def avg(error_list):
    sum = 0
    for item in error_list:
        sum += item
    return (sum/len(error_list))


def main():
    # the true result of the k-star query
    true_count = result(query)
    epsilons = [0.1, 0.5, 1.0]
    error_list = []
    time_list = []
    output = []
    output_time = []
    for epsilon in epsilons:
        for i in range(10):
            start = time.time()
            # the noise result of the k-star query
            re_query = rewrite_query(query, epsilon)
            noise_count = result(re_query)
            end = time.time()
            time_list.append(end - start)
            error_list.append(relative_error(true_count, noise_count))
        spendTime = avg(time_list)
        # convert to percentage
        error_avg = avg(error_list) * 100
        output.append(str(epsilon) + "/" + str(error_avg))
        output_time.append(str(epsilon) + "/" + str(spendTime))
    return (output, output_time)


if __name__ == "__main__":
    main()