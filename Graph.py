# This code is a test version for Company to find top important clients
# Implemented PageRank using pySpark


###########################################
# Use graph to score 3rd parties          #
# input: user/exchange/fact parquet data  #
# output: 3rd Parties with scores         #
###########################################


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from operator import add
from pyspark.sql.functions import desc

"""
Constants
"""

APP_NAME = "Page Rank 3rd Parties" # application name
EXCHANGE_ADDR = "maprfs:///dl.dev/data/fpi/processed/exchange/*.parquet" # location of dataset in Unix file path
USERS_ADDR = "maprfs:///dl.dev/data/fpi/processed/users/*.parquet"
FACT_ADDR = "maprfs:///dl.dev/data/fpi/processed/fact/*.parquet"
ITER_NUM = 10
DAMP = 0.85

"""
Helper functions
"""
def println(text):
	print text


"""
Main function
"""
def main(sc):
    """
    Load dataset
    """
    sqlContext = SQLContext(sc)
    exchange_table = sqlContext.read.parquet(EXCHANGE_ADDR)
    users_table = sqlContext.read.parquet(USERS_ADDR)
    fact_table = sqlContext.read.parquet(FACT_ADDR)
    print "---------------successfully load dataset----------------"

    users = users_table[["user_key","organization_id"]].withColumnRenamed("organization_id", "org_id_user")
    exchange = exchange_table[["exchange_key","organization_id"]].withColumnRenamed("organization_id","org_id_ex")
    fact = fact_table[["user_key", "exchange_key"]]
    
    print "---------------successfully selected data---------------"


    """
    Join dataset
    users: user_key, org_id_user
    exchange: exchange_key, org_id_ex
    fact: user_key, exchange_key
    method: 1. join users/exchange on fact, find all connections on pairs (inviter, invitee)=>(org_id_ex, org_id_user) 
            2. each pair means one action between 2 organizations
            3. note we are not going to count the internal actions
    """
    
    # left join user and fact
    users_fact = users.join(fact,users.user_key==fact.user_key,"left")
    # left join user_fact and exchange
    users_ex = users_fact.join(exchange, users_fact.exchange_key == exchange.exchange_key,"left")
    # select user_ex rows which have different org id and store them in cache
    contract_pairs = users_ex.filter(users_ex["org_id_user"] != users_ex["org_id_ex"])[["org_id_ex", "org_id_user"]].distinct()
    # calculate all members
    #allMembers = contract_pairs[["org_id_ex"]].unionAll(contract_pairs[["org_id_user"]])
    #org_num = allMembers.count()

    # filter 3rd parties because there sre clients in it 
    #third_parties = contract_pairs[["org_id_user"]].toDF("third_org_id_user")
    #clients = contract_pairs[["org_id_ex"]]
    #third_parties = third_parties.subtract(clients)

    print "---------------successfully joined data-----------------"

    """
    Page Rank Algorithm
    invitees
    inviters
    """
    pairs = contract_pairs.map(lambda pair:(pair[0],pair[1]))
    edges = pairs.groupByKey().mapValues(list)
    # inviter set
    inviters = pairs.map(lambda pair: pair[0]).distinct()
    # invitee set
    invitees = pairs.map(lambda pair: pair[1]).distinct()
    # total distinct org num
    org_num = inviters.union(invitees).distinct().count()
    # ranks we are going to compute
    ranks = invitees.map(lambda line: (line, 1))
    # org who didnt invite people
    danglerRanks = invitees.subtract(inviters).map(lambda x: (x, 1))
    # org who didnt be invited
    noInviters = inviters.subtract(invitees).map(lambda x: (x, 0))
    


    # iter over orgs and update their score
    for i in range(ITER_NUM):
    	change = edges.join(ranks).values().map(lambda (v, rank): [(i,rank/float(len(v))) for i in v]).flatMap(lambda x: x)
    	danglecontr = danglerRanks.join(ranks).map(lambda (v,(oldR,newR)): newR).reduce(lambda a,b: a+b)
    	ranks = change.reduceByKey(lambda a,b: a+b).union(noInviters).mapValues(lambda v: (1-DAMP)+DAMP*(v+ danglecontr/float(org_num)))
    	print "-------------------finished " + str(i) + " round---------------------"

    results = ranks.join(danglerRanks).sortBy(lambda x: -x[1][0])
    print "-------successfully finished page rank with top 3rd parties----------"
    print "org_id,score"
    for org, (score, _) in results.take(50):
    	print str(org)+","+str(score)

    sc.stop()



    
   



if __name__ == "__main__":

	# Configure Spark
    conf = SparkConf().setAppName(APP_NAME).set("spark.cores.max", "20")
    conf = conf.setMaster("spark://mapr-dev03.sncrbda.dev.vacum-np.sncrcorp.net:7077")
    sc    = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
