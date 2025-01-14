from tqdm import tqdm
from psycopg2._psycopg import AsIs
from fastgres.workload.workload import Workload
from fastgres.query_encoding import query as q
from fastgres.baseline.database_connection import DatabaseConnection


def get_wildcard_cardinality(db_connection: DatabaseConnection,
                             table: str, column: str, operator: str, filter_attribute: str):
    cursor = db_connection.cursor
    cursor.execute("SELECT COUNT(%s) FROM %s where %s %s %s",
                   (AsIs(column), AsIs(table), AsIs(column), AsIs(operator), filter_attribute))
    res = cursor.fetchall()[0][0]
    db_connection.close_connection()
    return res


def build_wildcard_dictionary(db_type_dict: dict, workload: Workload, db_connection: DatabaseConnection):
    wild_card_dict = dict()
    for query_name in tqdm(workload.queries):
        query = q.Query(query_name, workload)
        for table in query.attributes:
            max_v = 0
            for column in query.attributes[table]:
                for operator in query.attributes[table][column]:
                    char_test = (db_type_dict[table.lower()][column.lower()] == "character varying"
                                 or db_type_dict[table.lower()][column.lower()] == "character")
                    if (operator == "like" or operator == "ilike") and char_test:
                        filter_attribute = query.attributes[table][column][operator]

                        try:
                            wild_card_dict[table][column]
                        except KeyError:
                            wild_card_dict[table] = dict()
                        try:
                            wild_card_dict[table][column].keys()
                        except KeyError:
                            wild_card_dict[table][column] = dict()
                        # check if we would overwrite an entry beforehand
                        if filter_attribute in wild_card_dict[table][column].keys():
                            if wild_card_dict[table][column][filter_attribute] > 0:
                                continue

                        if max_v == 0:
                            cursor = db_connection.cursor
                            cursor.execute("SELECT COUNT(*) FROM {}".format(table))
                            max_v = cursor.fetchall()[0][0]
                            wild_card_dict[table]['max'] = max_v
                            db_connection.close_connection()

                        cardinality = get_wildcard_cardinality(db_connection, table, column, operator, filter_attribute)
                        if cardinality:
                            wild_card_dict[table][column][filter_attribute] = cardinality
    return wild_card_dict
