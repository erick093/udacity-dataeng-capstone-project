import matplotlib.pyplot as plt
import seaborn as sns
import logging
import re


def plot_missing_values(df):
    """
    Plot the missing values in a dataframe
    :param df:
    """
    missing_values = df.isnull().sum() / len(df)

    # plot using sns
    sns.set(style="whitegrid", rc={"figure.figsize": (12, 6)})
    sns.barplot(x=missing_values.index, y=missing_values.values)
    plt.axhline(y=0.8, color='k', linestyle='--')
    plt.title('Number of Missing Values in Each Column', fontsize=14)
    plt.xticks(rotation=70, fontsize=12)
    plt.show()


def clean_dataframe(df, columns_to_drop):
    """
    Clean the dataframe
    :param df: The dataframe to be cleaned
    :param columns_to_drop: The columns to be dropped
    :return: The cleaned dataframe
    """
    # Drop the columns
    df = df.drop(columns_to_drop, axis=1)

    # Drop the rows where all the values are missing
    df = df.dropna(how='all')

    return df


def clean_dataframe_in_spark(df, columns_to_drop):
    """
    Clean the dataframe in spark
    :param df: The dataframe to be cleaned
    :param columns_to_drop: The columns to be dropped
    :return: The cleaned dataframe
    """
    log_info('Cleaning the dataframe in spark, dataframe has {} rows and {} columns.'.format(df.count(),
                                                                                             len(df.columns)))
    # Drop the columns
    df = df.drop(*columns_to_drop)

    # Drop the rows where all the values are missing
    df = df.dropna(how='all')

    # Drop the duplicate rows
    # df = df.dropDuplicates(['cicid'])

    log_info('Dataframe cleaned, dataframe has {} rows and {} columns.'.format(df.count(),
                                                                               len(df.columns)))

    return df


def read_sas_labels_file(sas_filename, label):
    """
    Read labels from sas file
    :param sas_filename:
    :param label
    :return:
    """
    # open the sas label file
    with open(sas_filename) as f:
        lines = f.read()

    # filter the lines according to the label
    filtered_data = lines[lines.index(label):]
    filtered_data = filtered_data[:filtered_data.index(";")]

    # split the lines
    lines = filtered_data.split("\n")
    data = []

    for line in lines:
        if line.startswith("/*") or line.startswith("\%"):
            continue
        splits = line.split("=")
        if len(splits) != 2:
            continue

        # remove the leading and trailing spaces
        key = splits[0].strip().strip("'")
        value = splits[1].strip().strip("'")
        value = re.sub('\W,+', ' ', value).strip()

        # add the key and value to the list as a tuple
        data.append((key, value))

    return data


def init_logger():
    """
    Initialize the logger.
    """
    logging.basicConfig(level=logging.INFO,
                        format='====== %(asctime)s %(levelname)s %(message)s =====',
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler('export.log', mode='a')
                                  ],
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    logging.getLogger("info").setLevel(logging.INFO)
    logging.getLogger("error").setLevel(logging.ERROR)
    log_info("Logger initialized.")


def log_info(msg):
    """
    Log info message.
    :param msg: message to log
    """
    logging.getLogger("info").info(msg)


def log_error(msg):
    """
    Log error message.
    :param msg: error message to log
    :return:
    """
    logging.getLogger("error").error(msg)


def log(msg):
    """
    Log message as an info and error message.
    :param msg: message to log
    """
    log_info(msg)
    log_error(msg)


init_logger()
