def _init():
    #初始化一个全局的字典
    global _global_dict
    _global_dict = {}
def set_value(key,value):
    _global_dict[key] = value
    
def get_value(key):
    try:
        return _global_dict[key]
    except KeyError as e:
        print(e)