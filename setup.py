import sys
import os
import shutil
from distutils.core import setup
from Cython.Build import cythonize

currdir = os.path.abspath('.') + '/'
parentpath = sys.argv[1] if len(sys.argv) > 1 else ""
setupfile = os.path.join(os.path.abspath('.'), __file__)
build_dir = "build"
build_tmp_dir = build_dir + "/temp"

filter_dir_set = {'dist', 'build', 'configs', 'app', 'poseidon', 'utils', 'faultcode'}

except_files = {
    __file__,
    'main.py',
    'main_job.py',
    'setup.py',
    'gunicorn.config.py',
    'logging_config.py',
    'enos_get_data.py',
    os.path.join(currdir, 'test.py'),
    '.gitignore',
    '__init__.py',
    # os.path.join(currdir, 'data', 'get_data_async.py'),
    os.path.join(currdir, 'db', 'db.py')
}


def filter_file(file_name):
    if file_name.__contains__(currdir):
        file_name = file_name.replace(currdir, '')
    if file_name in except_files:  # 过滤文件
        return True
    for except_file in except_files:
        if file_name in except_file or file_name == except_file:
            return True
    file_path = file_name.split("/")
    if len(file_path) > 1:
        file_dir = ""
        for i in range(len(file_path) - 1):
            file_dir = os.path.join(file_dir, file_path[i])
            if file_dir in filter_dir_set:
                return True
    return file_path[0] in filter_dir_set


def getpy(basepath=os.path.abspath('.'), parentpath='', name='',
          copyOther=False, delC=False):
    """
    获取py文件的路径
    :param basepath: 根路径
    :param parentpath: 父路径
    :param name: 文件/夹
    :param copy: 是否copy其他文件
    :return: py文件的迭代器
    """
    fullpath = os.path.join(basepath, parentpath, name)
    for fname in os.listdir(fullpath):
        ffile = os.path.join(fullpath, fname)
        if os.path.isdir(ffile) and fname != build_dir and not fname.startswith('.'):
            for f in getpy(basepath, os.path.join(parentpath, name), fname,
                           copyOther, delC):
                yield f
        elif os.path.isfile(ffile):
            ext = os.path.splitext(fname)[1]
            # 删除.c 临时文件
            if ext == ".c":
                if delC:
                    os.remove(ffile)
            elif not filter_file(ffile) and (ext not in ('.pyc', '.pyx')
                                             and ext in ('.py', '.pyx')
                                             and not fname.startswith('__')):
                yield os.path.join(parentpath, name, fname)
            elif copyOther and ext not in ('.pyc', '.pyx') and fname != 'setup.py':  # 复制其他文件到./build 目录下
                dstdir = os.path.join(basepath, build_dir, parentpath, name)
                if not os.path.isdir(dstdir):
                    os.makedirs(dstdir)
                shutil.copyfile(ffile, os.path.join(dstdir, fname))
        else:
            pass


# 移动文件到对应目录下
def move_file(base_path=os.path.abspath('.'), parent_path='', name=''):
    full_path = os.path.join(base_path, parent_path, name)
    if os.path.isdir(full_path):
        if is_source(name):
            if len(os.listdir(full_path)) > 0:
                for source_file in os.listdir(full_path):
                    move_file(base_path, os.path.join(parent_path, name), source_file)
            else:
                build_base_path = os.path.join(os.path.abspath("."), "build")
                parent_dir = os.path.join(build_base_path, name)
                os.mkdir(parent_dir)
        else:
            pass
    elif os.path.isfile(full_path):
        build_base_path = os.path.join(os.path.abspath("."), "build")
        parent_dir = os.path.join(build_base_path, parent_path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)
        do_move_file(parent_path=parent_path, name=name)


def do_move_file(build_base_path=os.path.join(os.path.abspath("."), "build"), parent_path='', name=''):
    file_name = os.path.splitext(name)[0]
    for so_file in os.listdir(build_base_path):
        if so_file.startswith(file_name) and so_file.endswith(".so"):
            shutil.move(os.path.join(build_base_path, so_file), os.path.join(build_base_path, parent_path, so_file))
        else:
            pass


def is_source(source_file):
    return source_file != '__pycache__' and not source_file.startswith(".") and source_file != 'build'


# 删除build目录下的文件
if os.path.exists(build_dir):
    shutil.rmtree(build_dir)

# 删除log
if os.path.exists('logs'):
    shutil.rmtree('logs')
os.mkdir('logs')

# 获取py列表
module_set = set(getpy(basepath=currdir, parentpath=parentpath))

## 编译成.so文件
try:
    setup(ext_modules=cythonize(module_set, language_level=3, compiler_directives={'always_allow_keywords': True}),
          script_args=["build_ext", "-b", build_dir, "-t", build_tmp_dir])
    pass
except Exception as ex:
    print("error! ", str(ex))
else:
    # 复制其他文件到./build 目录下
    list(getpy(basepath=currdir, parentpath=parentpath, copyOther=True))

# 删除临时文件 ~
list(getpy(basepath=currdir, parentpath=parentpath, delC=True))

if os.path.exists(build_tmp_dir):
    shutil.rmtree(build_tmp_dir)

move_file()

print("Done !")
