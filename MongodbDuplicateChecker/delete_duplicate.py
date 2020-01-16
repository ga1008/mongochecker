import json
import os
import re
import sys

import pymongo
from pymongo.errors import OperationFailure
from tqdm import tqdm


class MongodbDuplicateChecker(object):
    def __init__(self, args):
        self.default_mos_path = 'mongodb_server.json'
        mos_file = self._mos_path(args)
        mos = self._get_mos(mos_file)
        mongodb_uri = self._get_uri(mos)
        self.client = pymongo.MongoClient(mongodb_uri)
        mongodb = self._db_name(mos)
        self.db = self.client[mongodb]
        self.collection = self._get_collection(mos)
        self.db_set = self.db[self.collection]
        self.check_keys = self._get_check_keys(mos)

    def start(self):
        print('---------- processing target [ {} ] ----------'.format(self.collection))
        db_data = self.db_set.find(no_cursor_timeout=True, batch_size=10000)
        self._process(db_data=db_data)
        print('--------- [ {} ] duplicate check done --------'.format(self.collection))

    def _process(self, db_data, check_only=False):
        counter = set()
        duplicate_data = 0
        del_success = 0
        total = 0
        find_dit = {x: 1 for x in self.check_keys}
        find_dit.update({"_id": 0})
        raw_data = db_data
        t = tqdm(total=db_data.count())
        for data in raw_data:
            d_lis = []
            for x in self.check_keys:
                value = eval('data["' + '"]["'.join(x.split('.')) + '"]')
                d_lis.append(str(value))
            d_str = '-'.join(d_lis)
            if d_str not in counter:
                counter.add(d_str)
            else:
                duplicate_data += 1
                if not check_only:
                    try:
                        d_count = self.db_set.delete_one(data)
                        del_success += d_count.deleted_count
                    except Exception as E:
                        print("delete err! {}".format(E))
                        continue
            t.update()
            total += 1
        t.close()
        if duplicate_data:
            print("done! total: {}, duplicate_data: {}, delete success: {}".format(total, duplicate_data, del_success))
        else:
            print("done! total: {}, no duplicate data found".format(total))

    def _get_check_keys(self, mos):
        check_keys = mos.get('check_keys')
        if check_keys:
            return check_keys
        doc = self.db_set.find_one()
        doc = dict(doc) if doc else dict()
        keys_lis = self._get_key_path(doc)
        print('------------ keys names in [ {} ]: ------------'.format(self.collection))
        for i, name in enumerate(keys_lis):
            print("[ {} ]: {}".format(i, name))
        print('--------------------------------------------------------')
        sel = input("input the nums of the keys to check duplicate(such as: 1,2,3), empty to cancel: ")
        if sel:
            sel = re.findall(r'\d+', sel)
            sel = [int(x) for x in sel if int(x) in range(len(keys_lis))] if sel else []
            return [keys_lis[x] for x in sel]
        else:
            ch = input('wrong input, do you want to input again?(y/n)').lower()
            if ch == 'y':
                self._get_check_keys(mos)
            else:
                sys.exit(1)

    def _get_key_path(self, dic, key_up='', sep='.'):
        """
        递归获取多层字典的所有的 key, 可以以指定的分割符组合
        :param dic:     源字典
        :param key_up:  上层键, 第一次传入是空字符
        :param sep:     上下层的键的分割符, 默认是 .
        :return:        返回键列表
        """
        se = list()
        for k, v in dic.items():
            i_k = "{}{}{}".format(key_up, sep, k) if key_up else k
            if isinstance(v, dict):
                se.extend(self._get_key_path(v, i_k, sep))
            else:
                se.append(i_k)
        return se

    def _get_collection(self, mos):
        names = mos.get('collection')
        if names:
            return names
        names = self.db.collection_names(include_system_collections=False)
        print("------------ collection names: ------------")
        for i, name in enumerate(names):
            print("[ {} ]: {}".format(i, name))
        print('------------------------------------------')
        sel = input("chose the num of the collection's name to process: ")
        sel = re.findall(r'\d+', sel)
        sel = int(sel[0]) if sel else None
        if sel not in [x for x in range(len(names))]:
            raise ValueError('wrong input ! ')
        return names[sel]

    def _db_name(self, mos):
        names = mos.get('db')
        if names:
            return names
        try:
            names = self.client.list_database_names()
            self._save_into_file(mos)
        except OperationFailure:
            print('the mongodb setting maybe wrong! please check it and restart')
            for k, v in mos.items():
                print("{}: {}".format(k, v))
            sys.exit(1)

        print("------------ database names: ------------")
        for i, name in enumerate(names):
            print("[ {} ]: {}".format(i, name))
        print('------------------------------------------')
        sel = input("chose the num of the database's name to process: ")
        sel = re.findall(r'\d+', sel)
        sel = int(sel[0]) if sel else None
        if sel not in [x for x in range(len(names))]:
            raise ValueError('wrong input ! ')
        return names[sel]

    def _mos_path(self, args):
        args = args or self.default_mos_path
        return args

    def _get_mos(self, mos_file):
        if os.path.exists(mos_file):
            with open(mos_file, 'r') as rf:
                mos_lis_raw = json.loads(rf.read())
            if isinstance(mos_lis_raw, dict):
                mos_lis_raw = [mos_lis_raw]
            print('---- servers in the {} file: ----'.format(mos_file))
            for i, dic in enumerate(mos_lis_raw):
                mos_info = "{}:{}, {}, {}".format(dic.get('host'), dic.get('port'), dic.get('name'), dic.get('source'))
                print("[ {} ]: {}".format(i, mos_info))
            print('---- ------------------------------------ ----')
            selection = input('chose the num of the server: ')
            selection = re.findall(r'\d+', selection)
            selection = int(selection[0]) if selection else None
            if selection not in [x for x in range(len(mos_lis_raw))]:
                raise ValueError('wrong input! ')
            mos_temp = mos_lis_raw[selection]
            return mos_temp
        else:
            print('mongodb_servers file not exits! ')
            inp = input('input the path to another file(p), or input mongodb setting(m): ').lower()
            if inp == 'p':
                m_path = input('path to mongodb setting: ')
                return self._get_mos(m_path)
            elif inp == 'm':
                m_server = input('server(127.0.0.1): ')
                m_port = input('port(27017): ') or '0'
                m_name = input('user(root): ')
                m_pwd = input('password(123456): ')
                m_source = input('source database(admin): ')

                m_dic = {"host": m_server or '127.0.0.1',
                         "port": int(m_port) or 27017,
                         "name": m_name or 'root',
                         "password": m_pwd or '123456',
                         "source": m_source or 'admin'
                         }
                return m_dic
            else:
                raise ValueError('wrong input! ')

    def _save_into_file(self, dic):
        with open(self.default_mos_path, 'w+') as wf:
            in_str = json.dumps([dic])
            wf.write(in_str)

    @staticmethod
    def _get_uri(mos):
        uri = 'mongodb://{}:{}@{}:{}/{}'.format(
            mos['name'], mos['password'], mos['host'], mos['port'], mos['source'])
        return uri

    def __del__(self):
        self.client.close()
        print('system exit')


def starter(args=None):
    args = args if args else (sys.argv[1] if len(sys.argv) > 1 else None)
    ck = MongodbDuplicateChecker(args)
    ck.start()


if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    starter(arg)
