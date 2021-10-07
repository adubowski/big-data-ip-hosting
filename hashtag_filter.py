from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode, desc

N_HASHTAGS = 20

# ToDo instead, a proper loading function should be used,
PATH = "/data/doina/Twitter-Archive.org/2017-01/01/01/*.json.bz2"

def most_used_hastags(df):
    """ Gets the most used hashtags from DataFrame

    Parameters
    ----------
    df : DataFrame
        DataFrame that contains only 2 columns: tweet 'id' and 'hashtag'.
        There should be 1 hashtag per row, with duplicate ids allowed
    Returns
    -------
    DataFrame
        DataFrame that contains the most used hashtags with the number of occurences
    """
    # Count and sort occurences, pick the top hashtags
    top_hs_df = hs_df.groupBy('hashtag').count().sort(desc('count')).limit(N_HASHTAGS)
    return top_hs_df

def remove_most_used_hashtags(hs_df, top_hs_df):
    """Filter out the most used hashtags from the main_df DataFrame

    Parameters
    ----------
    hs_df : DataFrame
        Contains all hashtags. Should contain at least 2 columns: 'hashtag' and tweet 'id',
        with only 1 hashtag per row.
    top_hs_df : DataFrame
        Contains the most used hashtags that should be removed from hs_df. Should at least contain column 'hashtag'
    Returns
    -------
    DataFrame
        Same as hs_df but without tweets that use only the most common hashtags
        Note, hashtags here are  1 by row, tweet ids are duplicated
    """

    filtered_df = hs_df.join(top_hs_df, ["hashtag"], "leftanti")
    return filtered_df

if __name__ == '__main__':
    # Set up
    sc = SparkContext(appName="hashtags")
    sc.setLogLevel("ERROR")
    spark = SparkSession(sc)
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    # Read the data
    df = spark.read.json(PATH)
    #Remove entries with null as id (my guess is that those entires are just empty)
    df = df.filter(df.id.isNotNull())
    # Explode hashtag texts. Here all necessary columns can be specified in the select statement
    hs_df = df.select('id', explode('entities.hashtags.text').alias('hashtag'))
    # Get most used hashtags
    top_hs_df = most_used_hastags(df)
    # Get id's of the tweets that don't use only the most common hashtags
    filtered_df = remove_most_used_hashtags(hs_df, top_hs_df)

    #Check if the code worked properly. This is very slow!!!
    if (filtered_df.select('hashtag').distinct().count() =
            df.select(explode('entities.hashtags.text')).distinct().count() - N_HASHTAGS):
        LOGGER.info("Hashtags are filtered properly")
    else:
        raise Exception("Hashtags are not filtered properly.")
