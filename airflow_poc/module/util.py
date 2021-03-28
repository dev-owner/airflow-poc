import os
from pprint import pprint


def utility(ds, **kwargs):
    print("utility test")
    print(ds)
    pprint(kwargs)
    return "utility test"


def get_dag_and_tag_id(filename):
    dag_id = os.path.basename(filename).replace(".py", "")
    tag_id = os.path.basename(os.path.dirname(filename))
    return dag_id, tag_id
