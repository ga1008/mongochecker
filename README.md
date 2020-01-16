移除 mongodb 的重复数据的小工具
====
### 安装: 

```shell script
$ pip install mongochecker
```

### 运行:  

```shell script
$ mongochecker [mongodb setting file path]
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
