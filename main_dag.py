import pandas as pd
from airflow import DAG
from datetime import datetime


default_args = {
    'owner': 'ryan',
    'depends_on_past': False,
    'email': ['ryan@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


def extract():
    import pandas as pd
    cs_researchers = pd.read_csv(
        '~/Documents/is3107-2/project/cs_researchers.csv')
    cs_researchers.apply(axis=1,
                         func=lambda x: download_a_xml(
                             pid=x.PID,
                             name=x.Name,
                         ))


def download_a_xml(pid, name):
    import requests
    import pandas as pd
    from os.path import exists
    path = "/home/ryan/Documents/is3107-2/project/"
    base_url = 'https://dblp.org/pid/'
    if not exists(path+"cs_researchers.csv"):
        raise Exception(
            "cs_research.csv required for this project does not exist")
    r = requests.get(base_url+pid+".xml")
    # I have created researchers_publication folder before hand.
    file_path = "/home/ryan/Documents/is3107-2/project/researchers_publications/" + \
        str(name)
    timestamp = "{}/{}/{}".format(datetime.now().year,
                                  datetime.now().month, datetime.now().day)
    pd.Series([timestamp]).to_csv(
        path+"timestamp.csv", index=False, header=False)

    with open(file_path, 'wb') as output_file:
        output_file.write(r.content)


def transform():
    import pandas as pd
    from os.path import exists
    import logging
    import glob

    log = logging.getLogger(__name__)
    log.info("transform()")
    path = "/home/ryan/Documents/is3107-2/project/"
    paths = glob.glob(path+"researchers_publications/*")

    columns = ["paper_key", "title", "year", "publication_type",
               "authors_pids", "authors_orcids",
               "authors_names", "category", "publisher", "position",
               "ee", "url", "crossref", "mdate", "is_seen"]
    if exists(path+"unique_data_year_and_category.csv"):
        unique_data_year_and_category = pd.read_csv(
            path+"unique_data_year_and_category.csv")
    else:
        unique_data_year_and_category = pd.DataFrame(
            columns=columns)

    unique_data_year_and_category['is_seen'] = False

    new_data_year_and_category = pd.DataFrame(
        columns=unique_data_year_and_category.columns)

    i = 0

    transform_a_xml(paths, i, unique_data_year_and_category,
                    new_data_year_and_category,
                    )


def transform_a_xml(paths, iteration,
                    unique_data_year_and_category,
                    new_data_year_and_category,
                    ):
    import xml.etree.ElementTree as ET
    import pandas as pd
    import logging
    log = logging.getLogger(__name__)
    with open(paths[iteration]) as file:
        text = file.readlines()
        if "xml" not in text[0]:
            log.info(paths[iteration] +
                     " is not a xml file, could be 404 error. Skipping.")
            iteration = iteration + 1
            if iteration >= len(paths):
                log.info("Ended early")
                save_to_csv(unique_data_year_and_category,
                            new_data_year_and_category)
            else:
                transform_a_xml(
                    paths, iteration,
                    unique_data_year_and_category,
                    new_data_year_and_category)
            return
    log.info("Processing file: "+paths[iteration])
    path = "/home/ryan/Documents/is3107-2/project/"

    tree = ET.parse(paths[iteration])
    root = tree.getroot()
    for publication in root:
        if publication.tag == 'coauthors':
            continue
        if publication.find('article') != None:
            publication = publication.find('article')
        elif publication.find('inproceedings') != None:
            publication = publication.find('inproceedings')
        elif publication.find('proceedings') != None:
            publication = publication.find('proceedings')
        elif publication.find('book') != None:
            publication = publication.find('book')
        elif publication.find('incollection') != None:
            publication = publication.find('incollection')
        elif publication.find('phdthesis') != None:
            publication = publication.find('phdthesis')
        elif publication.find('masterthesis') != None:
            publication = publication.find('masterthesis')
        elif publication.find('www') != None:
            publication = publication.find('www')
        elif publication.find('data') != None:
            publication = publication.find('data')
        else:
            continue
        if publication is None:
            continue
        publication_type = publication.tag\
            if publication.tag is not None else None

        # adding new paper_key and flaggin publication as seen.
        paper_key = publication.attrib['key'] \
            if 'key' in publication.attrib else None
        concat_year_and_category = True
        condition = unique_data_year_and_category.paper_key == paper_key
        if unique_data_year_and_category[condition].shape[0]:
            unique_data_year_and_category.is_seen = True
            log.info("year and category skipping")
            concat_year_and_category = False

        if paper_key is None:
            log.info("paper_key is none")
            continue

        title = publication.find('title').text \
            if publication.find('title') is not None else None
        year = int(publication.find('year').text)\
            if publication.find('year') is not None else None

        authors_pids = {}
        authors_orcids = {}
        authors_names = {}

        i = 1
        for e in publication:
            if e.tag == 'author' or e.tag == 'editor':
                if (e.tag == 'editor') :
                    log.warning("This is an editor")
                authors_pids.update(
                    {i: e.attrib['pid'] if 'pid' in e.attrib else None})
                authors_names.update(
                    {i: e.text if e.text is not None else None})
                authors_orcids.update(
                    {i: e.attrib['orcid'] if 'orcid' in e.attrib else None}
                )
                i = i + 1
        
        category = paper_key.split(
            '/')[0][0: -1] if paper_key is not None else None
        if publication.find('booktitle') != None:
            publisher = publication.find('booktitle').text\
                if publication.find('booktitle') is not None else None

        elif publication.find('journal') != None:
            publisher = publication.find('journal').text\
                if publication.find('journal') is not None else None

        elif publication.find('publisher') != None:
            publisher = publication.find('publisher').text\
                if publication.find('publisher') is not None else None

        else:
            publisher = None

        position = (publication.find('number').text
                    if publication.find('number') is not None else None,

                    publication.find('volume').text
                    if publication.find('volume') is not None else None,

                    publication.find('pages').text
                    if publication.find('pages') is not None else None)
        if len(publication.findall('ee')) > 1:
            ee = []
            for element in publication.findall('ee'):
                ee.append(element.text)
        elif len(publication.findall('ee')) == 1:
            ee = publication.find('ee').text
        elif publication.find('ee') is None:
            ee = None
        ee = str(ee)
        url = publication.find('url').text\
            if publication.find('url') is not None else None
        crossref = publication.find('crossref').text\
            if publication.find('crossref') is not None else None
        mdate = datetime\
            .strptime(publication.attrib['mdate'], "%Y-%m-%d")\
            .date()\
            if "mdate" in publication.attrib else None
        s = pd.DataFrame(data=[[paper_key,
                                title, year, publication_type, authors_pids,
                                authors_orcids, authors_names,
                                category, publisher, position, ee,
                                url, crossref, mdate, True
                                ]],
                         columns=unique_data_year_and_category.columns)
        # log.info("concat year and category {}".format(concat_year_and_category))
        if concat_year_and_category:
            new_data_year_and_category = pd.concat(
                [new_data_year_and_category, s],axis=0, ignore_index=True)

    iteration = iteration + 1
    if iteration >= len(paths):
        save_to_csv(unique_data_year_and_category,new_data_year_and_category)
    else:
        transform_a_xml(
            paths,
            iteration,
            unique_data_year_and_category,
            new_data_year_and_category)


def save_to_csv(unique_data_year_and_category,  new_data_year_and_category):
    import pandas as pd
    import logging
    log = logging.getLogger(__name__)
    path = "/home/ryan/Documents/is3107-2/project/"
    log.info("save_to_csv()")
    new_data_year_and_category.to_csv(
        path+"new_data_year_and_category.csv", index=False)

    to_be_deleted_data_year_and_cateogry = unique_data_year_and_category[
        unique_data_year_and_category.is_seen == False].iloc[:, :-1]
    to_be_deleted_data_year_and_cateogry.to_csv(
        path+"to_be_deleted_year_and_category.csv", index=False)

    unique_data_year_and_category = pd.concat(
        [unique_data_year_and_category, new_data_year_and_category],
        axis=0, ignore_index=True)
    unique_data_year_and_category = unique_data_year_and_category\
        .drop(to_be_deleted_data_year_and_cateogry.index)
    unique_data_year_and_category.to_csv(
        path+"unique_data_year_and_category.csv", index=False)


def q2_count_thing():
    import itertools
    import logging
    log = logging.getLogger(__name__)
    # count table thing
    unique_data_year_and_category = pd.read_csv(
        '~/Documents/is3107-2/project/unique_data_year_and_category.csv')
    cs_researchers = pd.read_csv(
        '~/Documents/is3107-2/project/cs_researchers.csv')
    cs_researchers = cs_researchers.PID
    combinations = list(itertools.combinations(list(cs_researchers), 2))
    columns = ['count', 'year', 'author1', 'author2']
    count_df = pd.DataFrame(columns=columns)
    for combo in combinations:
        pids = list(unique_data_year_and_category.authors_pids)
        index = [(combo[0] in pid and combo[1] in pid)
                 for pid in pids
                 ]
        df = unique_data_year_and_category[index].groupby("year").size()
        for i, row in df.iteritems():
            entry = pd.DataFrame(
                [[row, i, combo[0], combo[1]]],
                columns=columns)
            log.info("entry "+str(entry))
            count_df = pd.concat([count_df, entry])
    count_df.to_csv("~/Documents/is3107-2/project/count_df.csv")


def load_into_publication_by_year_and_category():
    statement = '''
        INSERT INTO publication_by_year_and_category (
        paper_key,title,year,type,authors_pids,
        authors_orcids,authors_names,
        category,publisher,position,ee,url,crossref,mdate)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    '''
    insert_all_into_db(statement, True)


def load_into_count_table():
    statement = '''
        INSERT INTO publication_count_unique_author_pair  (
        count,year,author1,author2) 
        VALUES (?,?,?,?)
    '''
    insert_all_into_db(statement, False)


def insert_all_into_db(cql_statement, is_Q1_table):
    import pandas as pd
    from cassandra.cluster import Cluster
    import ast
    import logging
    log = logging.getLogger(__name__)
    log.info("insert_all_into_db()")
    log.info("is Q1 table: {}".format(is_Q1_table))
    path = "/home/ryan/Documents/is3107-2/project/"

    cluster = Cluster()
    session = cluster.connect()
    row = session.execute("select release_version from system.local").one()
    if row:
        log.info(row[0])
    else:
        log.error("An error occurred.")
        session.shutdown()
        cluster.shutdown()
        raise Exception("Error occured while connection to db.")

    session.set_keyspace("is3107_project")
    insertion_statement = session.prepare(cql_statement)

    path_new = path+"new_data_year_and_category.csv" if is_Q1_table \
        else path + "count_df.csv"
    df = pd.read_csv(path_new, dtype={
        'paper_key': 'string',
        'title': 'string',
        'publication_type': 'string',
        'authors_pids': 'string',
        'authors_orcids': 'string',
        'authors_names': 'string',
        'category': 'string',
        'publisher': 'string',
        'position': 'string',
        'ee': 'string',
        'url': 'string',
        'crossref': 'string',
        'mdate': 'string'
    })
    if not is_Q1_table:
        path_delete = path+"to_be_deleted_year_and_category.csv"
        df_delete = pd.read_csv(path_delete, dtype={
            'paper_key': 'string',
            'title': 'string',
            'publication_type': 'string',
            'authors_pids': 'string',
            'authors_orcids': 'string',
            'authors_names': 'string',
            'category': 'string',
            'publisher': 'string',
            'position': 'string',
            'ee': 'string',
            'url': 'string',
            'crossref': 'string',
            'mdate': 'string'
        })

        for i in range(df_delete.shape[0]):
            a_list = df_delete.iloc[i, :]
            table = "publication_by_year_and_category"\
                if is_Q1_table else "publication_by_year"
            statement = "DELETE FROM "+table+" WHERE paper_key = "+a_list[0]
            prepared_statement = session.prepare(statement)
            session.execute(prepared_statement)
            log.info("Deleted data:" + a_list[1])
    else:
        df.drop('is_seen', inplace=True, axis=1)
        df.fillna("", inplace=True)
    for i in range(df.shape[0]):
        # For proper disemination of string representation of Json here:
        if is_Q1_table:

            a_list = df.iloc[i, :]
            a_list[4] = ast.literal_eval(a_list[4])
            a_list[5] = ast.literal_eval(a_list[5])
            a_list[6] = ast.literal_eval(a_list[6])
        else:
            a_list = df.iloc[i, 1:]
        session.execute(insertion_statement, a_list)

    session.shutdown()
    cluster.shutdown()


def log():
    import pandas as pd
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    import ast
    import logging
    log = logging.getLogger(__name__)
    log.info("log()")
    client_id = "rnIepDtyOeXasCLJPnywMvzn"
    secret = "L56TwBX,0G7l5lqOxlNfLN461ZBD0U-0lcOiH+HyxFQml2Z9c2mtBt7h22Kawn+MTweL51-iU37ZDJpwppCdKvM8fgUpbhZ2hlWJ9-nohS+7O1xrwnuP40ZyH.NXIPnk"
    path = "/home/ryan/Documents/is3107-2/project/"
    cloud_config = {
        'secure_connect_bundle':
            path+"secure-connect-is3107-project-by-ng-guangren-ryan.zip"
    }
    with open(path+"timestamp.csv") as file:
        timestamp = file.readline().rstrip()
    log.info(timestamp)
    auth_provider = PlainTextAuthProvider(client_id, secret)
    cluster_astra = Cluster(cloud=cloud_config,
                            auth_provider=auth_provider,
                            protocol_version=4)
    session_astra = cluster_astra.connect()
    row = session_astra.execute(
        "select release_version from system.local").one()
    if row:
        log.info(row[0])
    else:
        log.error("An error occurred.")
        session_astra.shutdown()
        cluster_astra.shutdown()
        raise Exception("Error occured while connection to db.")

    session_astra.set_keyspace("is3107_project")
    prepared_statement = session_astra.prepare('''
        INSERT INTO publication_update 
        (timestamp,title,authors,ee) 
        VALUES (?,?,?,?);
    ''')
    year_and_cat = pd.read_csv(path+"new_data_year_and_category.csv",
                               dtype={
                                   'paper_key': 'string',
                                   'title': 'string',
                                   'publication_type': 'string',
                                   'authors_pids': 'string',
                                   'authors_orcids': 'string',
                                   'authors_names': 'string',
                                   'category': 'string',
                                   'publisher': 'string',
                                   'position': 'string',
                                   'ee': 'string',
                                   'url': 'string',
                                   'crossref': 'string',
                                   'mdate': 'string'
                               })

    for i in range(year_and_cat.shape[0]):
        a_list = year_and_cat.iloc[i, :]
        authors = pd.DataFrame(columns=['name', 'orcid', 'pid'])
        log.info(a_list[6])
        authors.name = ast.literal_eval(a_list[6]).values
        authors.orcid = ast.literal_eval(a_list[5]).values
        authors.pid = ast.literal_eval(a_list[4]).values
        formated_authors = {}
        for i, author in authors.iterrows():
            formated_authors.update(
                {i: (author[0], author[1], author[2])})
        ee_str = a_list[10]
        if "[" in ee_str and "]" in ee_str:
            ee_str = ast.literal_eval(ee_str)
        else:
            ee_str = [ee_str]
        insertables = [
            timestamp,
            str(a_list[1]),
            formated_authors,
            ee_str
        ]
        log.info(insertables)
        session_astra.execute(prepared_statement, insertables)

    p = session_astra.prepare('''
        INSERT INTO volume_update 
        (timestamp, total_new, total_unique) 
        VALUES (?,?,?)
    ''')
    total_unique = pd\
        .read_csv(path+"unique_data_year_and_category.csv")\
        .shape[0]
    session_astra.execute(p, [timestamp, year_and_cat.shape[0], total_unique])


with DAG(
    dag_id='download_publications_data_dag',
    schedule_interval="@weekly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['is3107_project_dag'],
) as dag:
    from airflow.operators.python import PythonOperator
    #op = operator
    extract_op = PythonOperator(
        task_id='extract',
        python_callable=extract)
    tranform_op = PythonOperator(
        task_id="transform",
        python_callable=transform
    )
    q2_count_thing_op = PythonOperator(
        task_id="q2_count_thing_op",
        python_callable=q2_count_thing
    )
    load_into_pub_by_day_and_category_op = PythonOperator(
        task_id="load_into_pub_by_day_and_category",
        python_callable=load_into_publication_by_year_and_category)

    load_into_count_table_op = PythonOperator(
        task_id="load_into_count_table",
        python_callable=load_into_count_table)
    log_op = PythonOperator(
        task_id='log',
        python_callable=log
    )
    extract_op >> tranform_op >> load_into_pub_by_day_and_category_op >> log_op
    extract_op >> tranform_op >> q2_count_thing_op >> load_into_count_table_op



# path = "/home/ryan/Documents/is3107-2/project/"
# df = pd.read_csv(
#     path+"unique_data_year_and_category.csv")
# save_to_csv(df, pd.DataFrame())
