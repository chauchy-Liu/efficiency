from flask import jsonify


def success(data: object = None):
    """ 成功响应 默认值”成功“ """
    res = {
        'code': 200,
        'msg': '成功',
        'data': data
    }
    return jsonify(res)


def error(msg: str = "失败"):
    """ 失败响应 默认值“失败” """
    res = {
        'code': 200,
        'msg': msg,
        'data': {}
    }
    return jsonify(res)


def table_api(msg: str = "", count=0, data=None, limit=10):
    """ 动态表格渲染响应 """
    res = {
        'msg': msg,
        'code': 0,
        'data': data,
        'count': count,
        'limit': limit

    }
    return jsonify(res)
