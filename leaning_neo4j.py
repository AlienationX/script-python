# coding:utf-8
# python3

from py2neo import Graph, Node, Relationship
from py2neo.matching import NodeMatcher
from py2neo.bulk import create_nodes, merge_nodes, create_relationships, merge_relationships


def control_neo4j():
    # Graph("http://localhost:7474", auth=("username", "password"))
    graph = Graph("http://10.63.82.199:7474", auth=("neo4j", "admin"))
    # 显示版本
    print(graph.call.dbms.components())
    # 显示所有的node
    print(graph.schema.node_labels)
    # 显示所有的relationship
    print(graph.schema.relationship_types)
    # 所有node数量
    print(len(graph.nodes))

    # 1. cypher外壳, graph.run('a cypher expression'), 推荐，但是感觉很慢
    # print(graph.run("UNWIND range(1, 3) AS n RETURN n, n * n as n_sq"))
    #
    # cursor = graph.run("MATCH (n:Customer) RETURN n LIMIT 25")
    # # 返回游标或迭代器，使用一次就清空了，重复使用推荐赋值赋给个变量存储
    # print(cursor)
    # print(cursor.keys())
    # for res in cursor:
    #     print("contactName={:<20}, address={}".format(res["n"]["contactName"], res["n"]["address"]))
    #
    # # print(cursor.data())  # a list of dictionary
    # # print(cursor.to_data_frame())  # pd.DataFrame
    # # print(cursor.to_ndarray())  # numpy.ndarray
    # # print(cursor.to_subgraph())
    # # print(cursor.to_table())
    # print("-" * 60, "data")
    # print(graph.run("MATCH (n:Customer) RETURN n LIMIT 25").data())  # a list of dictionary
    # print("-" * 60, "data frame")
    # print(graph.run("MATCH (n:Customer) RETURN n LIMIT 25").to_data_frame())  # pd.DataFrame
    # print("-" * 60, "ndarray")
    # print(graph.run("MATCH (n:Customer) RETURN n LIMIT 25").to_ndarray())  # numpy.ndarray
    # print("-" * 60, "subgraph")
    # print(graph.run("MATCH (n:Customer) RETURN n LIMIT 25").to_subgraph())
    # print("-" * 60, "table")
    # print(graph.run("MATCH (n:Customer) RETURN n LIMIT 25").to_table())
    # print("-" * 60, "table end")
    #
    # cursor = graph.run("MATCH (n:Customer) RETURN n.contactName,n.age,n.companyName,n.city,n.address LIMIT 25")
    # # 会显示不全
    # print(cursor)
    #
    # # 很慢，比web查询慢很多很多
    # result = graph.run("MATCH p=()-[r:SUPPLIES]->() RETURN p LIMIT 25").data()
    # for row in result:
    #     print(row)

    # 2. py2neo方法
    # node1 = Node('Worker', name='Alice')
    # node2 = Node('Worker', name='Bob')
    #
    # node1['age'] = 20
    # node2['age'] = 25
    # node0_know_node1 = Relationship(node1, 'know', node2)
    # node0_know_node2 = Relationship(node2, 'friend', node1)
    #
    # graph.create(node1)
    # # graph.merge(node1, "Worker", "name")  # 存在则不创建（但是会报错，提示查找出比1个多，无语...）
    # graph.create(node2)
    # # graph.merge(node2, "Worker", "name")  # 存在则不创建（但是会报错，提示查找出比1个多，无语...）
    # graph.create(node0_know_node1)
    # graph.create(node0_know_node2)

    # print(node1.labels, node2.labels)
    # print(node1.items())
    # print(node1.keys())
    # print(node1.values())
    # print(len(node1))
    # print(dict(node1))
    # print(node1.graph)  # 返回节点绑定的graph的url地址
    # print(node1.identity)  # 返回节点在graph中的ID

    # nodes = NodeMatcher(graph)
    # result = nodes.match("Worker", name="Alice")
    # print(result)
    # print(len(result))
    # print(result.count())
    # print(result.all())  # [节点列表]

    # 3. 数据库血缘关系测试
    # columns_name = [
    #     "src_oder.oder_id",
    #     "src_oder.client_id",
    #     "src_oder.age",
    #     "src_oder_detail.charge_time",
    #     "src_oder_detail.catagory",
    #     "src_oder_detail.item_name",
    #     "src_oder_detail.fee",
    #
    #     "ods_oder.oder_id",
    #     "ods_oder.client_id",
    #     "ods_oder.age",
    #     "ods_oder_detail.charge_time",
    #     "ods_oder_detail.catagory",
    #     "ods_oder_detail.item_name",
    #     "ods_oder_detail.fee",
    #
    #     "dwb_oder.oder_id",
    #     "dwb_oder.client_id",
    #     "dwb_oder.age",
    #     "dwb_oder_detail.charge_time",
    #     "dwb_oder_detail.catagory",
    #     "dwb_oder_detail.item_name",
    #     "dwb_oder_detail.fee",
    #
    #     "dws_item.item_order_cnt",
    #     "dws_item.item_fee",
    #     "dws_item.item_18to24_classify_fee",
    # ]
    # for col in columns_name:
    #     # graph.create(Node("Column", name=col))  # 会重复创建
    #     graph.merge(Node("Column", name=col))  # node存在即会覆盖

    # nodes = NodeMatcher(graph)
    # graph.create(Relationship(nodes.match("Column", name="src_oder.oder_id").first(), 'reference', nodes.match("Column", name="ods_oder.oder_id").first()))
    # graph.create(Relationship(nodes.match("Column", name="ods_oder.oder_id").first(), 'reference', nodes.match("Column", name="dwb_oder.oder_id").first()))
    # graph.create(Relationship(nodes.match("Column", name="dwb_oder.oder_id").first(), 'reference', nodes.match("Column", name="dws_item.item_order_cnt").first()))
    #
    # graph.create(Relationship(nodes.match("Column", name="src_oder_detail.fee").first(), 'reference', nodes.match("Column", name="ods_oder_detail.fee").first()))
    # graph.create(Relationship(nodes.match("Column", name="ods_oder_detail.fee").first(), 'reference', nodes.match("Column", name="dwb_oder_detail.fee").first()))
    # graph.create(Relationship(nodes.match("Column", name="dwb_oder_detail.fee").first(), 'reference', nodes.match("Column", name="dws_item.item_fee").first()))
    #
    # graph.create(Relationship(nodes.match("Column", name="src_oder.age").first(), 'reference', nodes.match("Column", name="ods_oder.age").first()))
    # graph.create(Relationship(nodes.match("Column", name="ods_oder.age").first(), 'reference', nodes.match("Column", name="dwb_oder.age").first()))
    # graph.create(Relationship(nodes.match("Column", name="src_oder_detail.catagory").first(), 'reference', nodes.match("Column", name="ods_oder_detail.catagory").first()))
    # graph.create(Relationship(nodes.match("Column", name="ods_oder_detail.catagory").first(), 'reference', nodes.match("Column", name="dwb_oder_detail.catagory").first()))
    # graph.create(Relationship(nodes.match("Column", name="dwb_oder.age").first(), 'reference', nodes.match("Column", name="dws_item.item_18to24_classify_fee").first()))
    # graph.create(Relationship(nodes.match("Column", name="dwb_oder_detail.catagory").first(), 'reference', nodes.match("Column", name="dws_item.item_18to24_classify_fee").first()))
    # graph.create(Relationship(nodes.match("Column", name="dwb_oder_detail.fee").first(), 'reference', nodes.match("Column", name="dws_item.item_18to24_classify_fee").first()))
    #
    # graph.create(Relationship(nodes.match("Column", name="src_oder.client_id").first(), 'reference', nodes.match("Column", name="ods_oder.client_id").first()))
    # graph.create(Relationship(nodes.match("Column", name="ods_oder.client_id").first(), 'reference', nodes.match("Column", name="dwb_oder.client_id").first()))
    #
    # graph.create(Relationship(nodes.match("Column", name="src_oder_detail.charge_time").first(), 'reference', nodes.match("Column", name="ods_oder_detail.charge_time").first()))
    # graph.create(Relationship(nodes.match("Column", name="ods_oder_detail.charge_time").first(), 'reference', nodes.match("Column", name="dwb_oder_detail.charge_time").first()))

    # 查询所有reference关系
    # for row in graph.run("MATCH p=()-[r:reference]->() RETURN p LIMIT 25").data():
    #     print(row)

    # 递归查找某节点的所有依赖关系, 无敌语句
    # to_data_frame()会显示为空白，显示的问题？还不清楚
    # for row in graph.run("match data=(a:Column)-[:reference*1..6]->(c:Column {name:'dws_item.item_order_cnt'}) return data").data():
    #     print(row)
    #
    # print("-" * 20)
    # search_column_name = "dws_item.item_18to24_classify_fee"
    # for row in graph.run("match data=(a:Column)-[*1..6]->(c:Column {name:'%s'}) return data" % search_column_name).data():
    #     print(row)

    # 4. import data (疾病和项目的关系)
    # keys = ["code", "name", "level"]
    # data = [
    #     ["01", "高血压", "1"],
    #     ["02", "上呼吸道感染", "1"],
    #     ["03", "冠状动脉粥样硬化性心脏病", "1"],
    # ]
    # create_nodes(graph.auto(), data, labels={"Disease"}, keys=keys)
    # print(graph.nodes.match("Disease").count())
    #
    # data = [
    #     {"code": "04", "name": "脑梗死", "level": "2"},
    #     {"code": "05", "name": "支气管炎", "date_of_birth": "1943-10-01"},
    #     {"code": "06", "name": "糖尿病"},
    # ]
    # create_nodes(graph.auto(), data, labels={"Disease"})
    # print(graph.nodes.match("Disease").count())
    #
    # data = [
    #     {"code": "06", "name": "糖尿病", "flag": "update"},  # 没有的属性会增加，应该是整行数据的覆盖
    #     {"code": "07", "name": "感冒", "level": "5"},
    #     {"code": "08", "name": "急性上呼吸道感染", "date_of_birth": "1956-01-01"},
    # ]
    # merge_nodes(graph.auto(), data, ("Disease", "name"))
    # print(graph.nodes.match("Disease").count())

    # keys = ["code", "name"]
    # data = [
    #     ["01", "131碘 - 功能自主状甲状腺瘤治疗"],
    #     ["02", "131碘 - 甲亢治疗"],
    #     ["03", "131碘 - 甲状腺癌转移灶治疗"],
    #     ["04", "13价肺炎球菌多糖结合疫苗"],
    #     ["05", "13碳尿素呼气试验"],
    #     ["06", "18种氨基酸注射液"],
    #     ["07", "23价肺炎球菌多糖疫苗"],
    #     ["08", "PPD检查"],
    #     ["09", "TT病毒抗体检测"],
    #     ["10", "阿立哌唑胶囊"],
    #     ["11", "阿立哌唑口崩片"]
    # ]
    # create_nodes(graph.auto(), data, labels={"Item"}, keys=keys)
    # print(graph.nodes.match("Item").count())

    keys = ["disease", "item", "cnt"]
    data = [
        ["高血压", "131碘 - 功能自主状甲状腺瘤治疗", "1"],
        ["糖尿病", "131碘 - 甲亢治疗", "2"],
        ["高血压", "131碘 - 甲亢治疗", "2"],
        ["高血压", "131碘 - 甲状腺癌转移灶治疗", "1"],
        ["高血压", "13价肺炎球菌多糖结合疫苗", "1"],
        ["糖尿病", "13碳尿素呼气试验", "3"],
        ["高血压", "13碳尿素呼气试验", "3"],
        ["支气管炎", "13碳尿素呼气试验", "3"],
        ["支气管炎", "18种氨基酸注射液", "2"],
        ["高血压", "18种氨基酸注射液", "2"],
        ["高血压", "23价肺炎球菌多糖疫苗", "3"],
        ["糖尿病", "23价肺炎球菌多糖疫苗", "3"],
        ["支气管炎", "23价肺炎球菌多糖疫苗", "3"],
        ["支气管炎", "PPD检查", "1"],
        ["支气管炎", "TT病毒抗体检测", "1"],
        ["糖尿病", "阿立哌唑胶囊", "1"],
        ["糖尿病", "阿立哌唑口崩片", "1"]
    ]
    # create_nodes(graph.auto(), data, labels={"DiseaseWithItem"}, keys=keys)
    print(graph.nodes.match("DiseaseWithItem").count())

    # 生成关联关系数据，关联关系数据可以不用入库（如果使用CQL生成关联关系，推荐入库）
    # data_reference = [((x[0]), {}, x[1]) for x in data]                     # 中间的字典可加也可不加
    # data_reference = [(x[0], {"since": "2000-01-01"}, x[1]) for x in data]
    # print(data_reference)
    # create_relationships(graph.auto(), data_reference, "Usage_Of_Medication", start_node_key=("Disease", "name"), end_node_key=("Item", "name"))

    # 重复导入会生成重复关系，需要使用merge

    # 推荐
    # data_reference = [(x[0], [], x[1]) for x in data]
    # print(data_reference)
    # merge_relationships(graph.auto(), data_reference, merge_key="Usage_Of_Medication", start_node_key=("Disease", "name"), end_node_key=("Item", "name"), keys=[])

    # data_reference = [(x[0], ["1990-01-01", "9999-12-31"], x[1]) for x in data]
    # print(data_reference)
    # merge_relationships(graph.auto(), data_reference, merge_key=("Usage_Of_Medication",), start_node_key=("Disease", "name"), end_node_key=("Item", "name"), keys=["since", "end"])

    # 推荐
    data_reference = [(x[0], {"since": "1990-01-01", "end": "9999-12-31"}, x[1]) for x in data]
    print(data_reference)
    merge_relationships(graph.auto(), data_reference, merge_key=("Usage_Of_Medication",), start_node_key=("Disease", "name"), end_node_key=("Item", "name"))


if __name__ == "__main__":
    control_neo4j()
