__author__ = 'sogolmoshtaghi'

import os
import sys


# Work-around for resolving pyspark import issues in pyCharm
os.environ['SPARK_HOME'] = '/Users/sogolmoshtaghi/spark-1.2.1-bin-hadoop1'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
import pyspark as ps


def user_phone(user):
    """
    Returns a tuple of ID and list of phone numbers associated to that ID.
    :param user: Represents a user in the user file
    """
    phones = []
    for i in range(2, len(user)):
        if not '@'in user[i] and not '.' in user[i]:
            phone_lst = user[i].split(',')
            for phone in phone_lst:
                phones.append(phone)
    return user[0], phones


def process_users(users):
    """
    Returns two RDDs based on users.txt file.
    :return: user_name_rdd -> (user_ID, name), user_phone_rdd -> (user_ID, phone_list)
    """
    users = users.map(lambda x: x.split(';'))
    user_phone_rdd = users.map(user_phone)
    user_name_rdd = users.map(lambda x: (x[0], x[1]))
    return user_name_rdd, user_phone_rdd


def process_transactions(trans):
    """
    Processes transaction text file.
    :param sc: SparkContext
    :return: RDD of customer_id mapped to transaction amount sorted by transaction amount
    """
    trans = trans.map(lambda x: x.split(';'))
    trans = trans.filter(lambda x: x[2][:4] == '2015')
    trans = trans.map(lambda x: (x[0], x[1]))
    trans_without_dollar = trans.map(lambda x: (x[0], float(x[1][1:])))
    # tuple of id to a string of all transaction amounts
    by_cust_id = trans_without_dollar.reduceByKey(lambda x, y: x+y)
    return by_cust_id


def filter_phones(donot_rdd, phone_rdd):
    """
    Filters phone_rdd based on the numbers present in the donot_rdd
    :param donot_rdd: RDD based on donot file consisted of tuples (phone, 1)
    :param phone_rdd: RDD of tuples of user_id, phone_list
    :return: RDD of user_id, phone_list in which phone_list is filtered.
    NOTE: donotcall.txt could simply be read into a set and broadcast it to workers.
    But if the donot call list was larger, then reading it into an RDD (current solution) would be more
    efficient.
    """
    phone_user = phone_rdd.flatMap(lambda tup: [(phone, (tup[0])) for phone in tup[1]])

    valid_phone_users = phone_user.subtractByKey(donot_rdd)
    valid_phone_users = valid_phone_users.filter(lambda x: x[0] != '')
    valid_user_phone = valid_phone_users.map(lambda x: (x[1], x[0]))
    valid_user_phone = valid_user_phone.groupByKey().map(lambda x: (x[0], list(x[1])))
    return valid_user_phone


def join_users_transactions(users, trans):
    """
    Joins the users and transaction RDDs and sorts the resulting RDD by transaction amount.
    :param users: RDD -> [(user_id, (name, [phones])]
    :param trans: RDD -> [(user_id, transaction_amount)]
    :return: RDD -> [(user_id, (name, [phones], transaction amount))]
    """
    campaign = users.join(trans)
    campaign = campaign.sortBy(lambda x: x[1][1], ascending=False)
    return campaign


def process_pipeline():
    """
    Main func
    :return:
    """
    sc = ps.SparkContext('local[4]')
    users = sc.textFile('users.txt')
    trans = sc.textFile('transactions.txt')
    donot = sc.textFile('donotcall.txt')
    name_rdd, phone_rdd = process_users(users)
    name_rdd.persist()
    phone_rdd.persist()
    donot_rdd = donot.map(lambda x: (x, 1))
    valid_user_phones = filter_phones(donot_rdd, phone_rdd)
    valid_user_phones.persist()
    p_users = name_rdd.join(valid_user_phones)
    p_users.persist()

    p_trans = process_transactions(trans)
    p_trans.persist()

    campaign_rdd = join_users_transactions(p_users, p_trans)
    top_campaign = campaign_rdd.take(1000)
    write_lst(top_campaign)


def write_lst(top_camp):
    """
    Writes the items list to a file in the following format:
    Customer ID, Customer name, phone list that can be used to contact the user, total transaction amount
    :param top_camp: list containing the top 1000 users based on transaction amounts.
    """
    f = open('top_campaigns.txt', 'w')
    for item in top_camp:
        f.write(item[0]+", ")               #ID
        f.write(item[1][0][0]+", ")         # Name
        f.write(", ".join(item[1][0][1]))   # List of phones
        f.write(", %s\n" % str(item[1][1])) # Transaction amount
    f.close()


if __name__ == '__main__':
    process_pipeline()



