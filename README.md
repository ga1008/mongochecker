移除 mongodb 的重复数据的小工具
====

 - ### 可以去除重复数据 >>> mongochecker
 - ### 可以复制数据 >>> mongocopy

### 安装: 

```shell script
$ pip install mongocheck
```

## 1. 去重功能介绍:

### 运行:  

```shell script
$ mongocheck [mongodb setting file path]
```

### mongodb setting file 格式:

```json
[
    {"host": "127.0.0.1",
      "port": 27017,
      "name": "root",
      "password": "123456",
      "source": "admin",
      "db": "mydb1"
    },
    {"host": "111.123.234.321",
      "port": 27017,
      "name": "root",
      "password": "123456",
      "source": "admin",
      "db": "mydb1",
      "collection": "my_collection",
      "check_keys": ["key1", "key2"]
    }
]
```

### 说明:

```
 1. mongodb setting file 必须是 json 格式
 2. 一个 mongodb setting file 可以是一个 mongodb 设置的字典, 也可以是多个mongodb 设置组成的列表
 3. 配置里 host 和 port 是必须的, 如果开启了认证, 则 name, password, source 也是必须有的
 4. 除了上述的必须配置, db 是用来指定操作的数据库的, collection 是用来指定操作的集合的, check_keys 是用来指定集合过滤字段的
 5. 也可以不指定 mongodb setting file, 运行时跟着提示填写即可
```

## 数据贵无价 操作需谨慎


## 2. 复制功能介绍:


### 运行:  

```shell script
$ mongocopy [mongodb copy setting file path]
```

### mongodb copy setting file 格式:

```json
{
  "from": {
    "host": "127.0.0.1",
    "port": 27017,
    "user": "root",
    "password": "123456",
    "source": "admin",
    "db": "source_db",
    "from_collection": "source_collection",
    "condition": {
      "class": "1"
    }
  },
  "to": {
    "host": "127.0.0.1",
    "port": 27017,
    "user": "root",
    "password": "123456",
    "source": "admin",
    "db": "target_db",
    "to_collection": "target_collection",
    "filter": ["key0.key01", "key2"]
  }
}
```

### 说明:

```
 1. 同样必须是 json 格式
 2. 一个 setting file 包含两个部分, "from" 是源数据库, "to" 是目标数据库
 3. "source" 是 mongodb 的验证数据库, 开启验证后的数据库连接时需要指定
 4. "from" 里的 "condition" 值是个字典, 即复制符合指定条件的数据
 5. "to" 里的 "filter" 是过滤字段, 如果目标数据库里有这些字段并且和插入值一样的时候, 就会被过滤, 保证不重复
 6. setting file 里的字段都是可有可无的, 没有的话运行时跟着提示填写即可
 7. setting file 本身也可以不指定
```

## 数据贵无价 操作需谨慎

