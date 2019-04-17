#!/usr/bin/env python
# coding=utf-8

import requests
import json
import time
from datetime import datetime, date

# project ==> model ==> cube
# 相应的值如下
# pet_medical ==> pay ==> cube_pay

HOST = "http://localhost:7070"
USER = "ADMIN"
PASS = "KYLIN"

# 或者使用base64加密放到header里面  没测试
HEADERS = "Authorization: Basic QURNSU46S1lMSU4="


def auth():
    # r = requests.post(HOST + "/kylin/api/user/authentication", headers=HEADERS) # 没测试通
    r = requests.get(HOST + "/kylin/api/user/authentication", auth=(USER, PASS))
    print(r.status_code)
    print(r.ok)
    # print(r.text)
    print(r.json())
    print(json.dumps(r.json(), indent=4))


def get_kylin_info():
    # 获取project里面的所有字段信息，可以理解为mysql元数据的information_schema.columns表
    r = requests.get(HOST + "/kylin/api/tables_and_columns?project={project}".format(project="pet_medical"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # 获取所有cube信息
    r = requests.get(HOST + "/kylin/api/cubes", auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # 获取指定cube概括信息（重点：包含事实表的维度外键的最小值和最大值，及所有segments信息）
    # segments的status如果为 NEW （不是READY），则需要获取last_build_job_id进行api构建
    # r = requests.get(HOST + "/kylin/api/cubes/{}".format("cube_pay"), auth=(USER, PASS))
    r = requests.get(HOST + "/kylin/api/cubes?cubeName={cube_name}".format(cube_name="cube_pay"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # 获取指定cube描述信息（重要：包含所有维度和度量字段相关信息）
    r = requests.get(HOST + "/kylin/api/cube_desc/{cube_name}".format(cube_name="cube_pay"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # 获取指定model相关信息
    r = requests.get(HOST + "/kylin/api/model/{model_name}".format(model_name="pay"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # 获取指定项目下的所有table相关信息（重点：包换字段的类型过和注释内容）
    r = requests.get(HOST + "/kylin/api/tables?project={project}".format(project="pet_medical"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # 获取指定项目下的指定table相关信息
    # r = requests.get(HOST + "/kylin/api/tables/pet_medical/pet_medical.dim_date".format(project="pet_medical"), auth=(USER, PASS))
    r = requests.get(HOST + "/kylin/api/tables/{project}/{table_name}".format(project="pet_medical", table_name="pet_medical.dim_date"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))


def workflow_build():
    # bash shell 构建
    # curl -X PUT -H "Authorization: Basic QURNSU46S1lMSU4=" -H 'Content-Type: application/json' -d '{"endTime":1555344000000, "buildType":"BUILD"}' http://10.15.1.49:7070/kylin/api/cubes/cube_pay/rebuild
    # curl -X PUT -H "Authorization: Basic QURNSU46S1lMSU4=" -H 'Content-Type: application/json' -d '{"startTime":0, "endTime":1555344000000, "buildType":"BUILD"}' http://10.15.1.49:7070/kylin/api/cubes/cube_pay/rebuild

    start_date = datetime.strptime("2000-01-01", "%Y-%m-%d")
    start_timestamp = int(time.mktime(start_date.timetuple()) * 1000)
    # start_timestamp = 0
    end_date = date.today()
    end_timestamp = int(time.mktime(end_date.timetuple()) * 1000)
    data = {
        "startTime": start_timestamp,
        "endTime": end_timestamp,
        "buildType": "BUILD"  # BUILD / MERGE / REFRESH
    }
    print(data)
    # ======> cube

    # 构建指定cube build cube （重点：必须传入json关键字，如果传入dict则需要转换成json，data=data会报错）
    # headers中的content-type，默认为application/json，所以需要使用json参数（极力推荐），如果使用data参数，需要修改headers的content-type。
    r = requests.put(HOST + "/kylin/api/cubes/{cube_name}/rebuild".format(cube_name="cube_pay"), json=data, auth=(USER, PASS))
    # r = requests.put(HOST + "/kylin/api/cubes/{cube_name}/rebuild".format(cube_name="cube_pay"), data=json.dumps(data), auth=(USER, PASS))  # 该方法需要设置headers的content-type为application/x-www-form-urlencoded
    print(json.dumps(r.json(), indent=4))

    # enable disable purge
    # r = requests.put(HOST + "/kylin/api/cubes/{cube_name}/enable".format(cube_name="cube_pay"), data=data, auth=(USER, PASS))
    # r = requests.put(HOST + "/kylin/api/cubes/{cube_name}/disable".format(cube_name="cube_pay"), data=data, auth=(USER, PASS))
    # r = requests.put(HOST + "/kylin/api/cubes/{cube_name}/purge".format(cube_name="cube_pay"), data=data, auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # ======> job

    # 获取job list, 符合条件会返回多条。参数如下:
    # cubeName - optional string Cube name.
    # projectName - required string Project name.
    # status - optional int Job status, e.g. (NEW: 0, PENDING: 1, RUNNING: 2, STOPPED: 32, FINISHED: 4, ERROR: 8, DISCARDED: 16)
    # offset - required int Offset used by pagination.
    # limit - required int Jobs per page.
    # timeFilter - required int, e.g. (LAST ONE DAY: 0, LAST ONE WEEK: 1, LAST ONE MONTH: 2, LAST ONE YEAR: 3, ALL: 4)

    # r = requests.get(HOST + "/kylin/api/jobs", auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))
    # params = {
    #     "cubeName": "cube_pay",
    #     "projectName": "pet_medical",
    # }
    # r = requests.get(HOST + "/kylin/api/jobs", params=params, auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))
    # job_uuid = (r.json())[0]["uuid"]
    # print(job_uuid)

    # 获取指定job信息, 返回的 job_status 代表job的当前状态。RUNNING / FINISHED ...
    # r = requests.get(HOST + "/kylin/api/jobs/{job_uuid}".format(job_uuid="5f20e98b-60f5-2672-8a9b-2e8b82e438fa"), auth=(USER, PASS))
    # r = requests.get(HOST + "/kylin/api/jobs/{job_uuid}".format(job_uuid=job_uuid), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # 重新执行 resume job
    # r = requests.put(HOST + "/kylin/api/jobs/{job_uuid}/resume".format(job_uuid="5f20e98b-60f5-2672-8a9b-2e8b82e438fa"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # pause cancel
    # r = requests.put(HOST + "/kylin/api/jobs/{job_uuid}/pause".format(job_uuid="5f20e98b-60f5-2672-8a9b-2e8b82e438fa"), auth=(USER, PASS))
    # r = requests.put(HOST + "/kylin/api/jobs/{job_uuid}/cancel".format(job_uuid="5f20e98b-60f5-2672-8a9b-2e8b82e438fa"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))

    # Get job step output
    # GET /kylin/api/jobs/{jobId}/steps/{stepId}/output

    # ======> segment

    # delete Segment
    # r = requests.delete(HOST + "/kylin/api/cubes/{cube_name}/segs/{segment_name}".format(cube_name="cube_pay", segment_name="asdf"), auth=(USER, PASS))
    # print(json.dumps(r.json(), indent=4))


if __name__ == "__main__":
    # auth()
    # get_kylin_info()
    workflow_build()
