import json
import os
import re
import sys
import time

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
        names = self.db.list_collection_names(include_system_collections=False)
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


class MongodbCopy(object):
    def __init__(self, args=None):
        self.default_mos_path = self._mos_path(args, 'mongodb_copy_set.json')
        self.fromdb_collection = None
        self.fromdb_client = None
        self.fromdb = None
        self.fromdb_set = None
        self.todb_collection = None
        self.todb_client = None
        self.todb = None
        self.todb_set = None
        self.condition = None
        self.f_mos = self._get_from_mos()
        self.t_mos = self._get_to_mos()
        self.filter = self._get_filter()
        self._save_mos(self.f_mos, self.t_mos)

    def start_copy(self):
        to_data = {}
        to_data_set = set()
        if self.filter:
            to_data = self.todb_set.find()
            print('------------- preparing the "to" data ------------')
            tt = tqdm(total=self.todb_set.count_documents({}))
            for data in to_data:
                tt.update()
                try:
                    d_lis = []
                    for x in self.filter:
                        value = eval('data["' + '"]["'.join(x.split('.')) + '"]')
                        d_lis.append(str(value))
                    d_str = '>'.join(d_lis)
                    to_data_set.add(d_str)
                except:
                    pass
            tt.close()
            time.sleep(0.1)
            print('-------------- "to" data ready -------------------')
        from_data = self.fromdb_set.find(self.condition)
        from_count = self.fromdb_set.count_documents(self.condition)
        print('------------------ start copy ---------------------')
        time.sleep(0.1)
        t = tqdm(total=from_count)
        count = 0
        dup_count = 0
        for d in from_data:
            try:
                d_key = []
                for x in self.filter:
                    value = eval('d["' + '"]["'.join(x.split('.')) + '"]')
                    d_key.append(str(value))
                d_key = '>'.join(d_key)
            except:
                d_key = ">".format([x for x in d.values()])
            if d_key not in to_data_set:
                try:
                    self.todb_set.insert_one(d)
                    count += 1
                except Exception as E:
                    pass
            else:
                dup_count += 1
            t.update()
        t.close()
        time.sleep(0.1)
        print('------------------ process done ---------------------')
        print('copy to [ {} ] done! total: [ {} ], success: [ {} ], duplicate [ {} ]'.format(
            self.todb_collection, from_count, count, dup_count))

    def _get_filter(self):
        filter_ori = self.t_mos.get('filter')
        i_info = "input check keys(input 'i' to direct insert, empty to show all keys): "
        if filter_ori:
            i_info = "input check keys({}, input 'i' to direct insert, empty to show all keys): ".format(filter_ori)
        sel = input(i_info)
        if sel.lower() == 'i':
            return
        if sel:
            try:
                sel = json.loads(sel)
            except:
                print('wrong input, make sure it is a json format')
                time.sleep(1)
                return self._get_filter()
        else:
            sel = self._get_check_keys(self.t_mos)
            if not sel:
                return self._get_filter()
        return sel

    def _get_from_mos(self):
        print(' -------------- copy data from: ---------------')
        if os.path.exists(self.default_mos_path):
            with open(self.default_mos_path, 'r') as rf:
                mos_raw = json.loads(rf.read())
            f_md = mos_raw.get('from') or {}
            f_host = f_md.get('host') or input('host({}): '.format('127.0.0.1'))
            f_port = int(f_md.get('port') or input('port({}): '.format('27017')))
            f_user = f_md.get('user') or input('user({}): '.format('root'))
            f_pwd = f_md.get('password') or input('password({}): '.format('123456'))
            f_source = f_md.get('source') or input('source({}): '.format('admin'))
            self.fromdb_client = pymongo.MongoClient(self._format_uri(f_host, f_port, f_user, f_pwd, f_source))
            f_db = f_md.get('db') or self._show_dbs(db=self.fromdb_client)
            self.fromdb = self.fromdb_client[f_db]
            f_col = f_md.get('from_collection') or self._show_clos(db=self.fromdb)
            self.fromdb_collection = f_col
            self.fromdb_set = self.fromdb[self.fromdb_collection]
            con = f_md.get('condition') or self._get_filer(self.fromdb_set, self.fromdb_collection)
            self.condition = con
        else:
            f_host = input('host({}): '.format('127.0.0.1')) or '127.0.0.1'
            f_port = int(input('port({}): '.format('27017')) or '27017')
            f_user = input('user({}): '.format('root')) or 'root'
            f_pwd = input('password({}): '.format('123456')) or '123456'
            f_source = input('source({}): '.format('admin')) or 'admin'
            self.fromdb_client = pymongo.MongoClient(self._format_uri(f_host, f_port, f_user, f_pwd, f_source))
            f_db = self._show_dbs(db=self.fromdb_client)
            self.fromdb = self.fromdb_client[f_db]
            f_col = self._show_clos(db=self.fromdb)
            self.fromdb_collection = f_col
            self.fromdb_set = self.fromdb[self.fromdb_collection]
            con = self._get_filer(self.fromdb_set, self.fromdb_collection)
            self.condition = con

        dic = {'host': f_host, 'port': f_port, 'user': f_user, 'password': f_pwd, 'source': f_source, 'db': f_db, 'from_collection': f_col, 'condition': con}
        return dic

    def _get_to_mos(self):
        print(' -------------- copy data to: ---------------')
        if os.path.exists(self.default_mos_path):
            with open(self.default_mos_path, 'r') as rf:
                mos_raw = json.loads(rf.read())
            t_md = mos_raw.get('to') or {}
            t_host = t_md.get('host') or input('host({}): '.format('127.0.0.1'))
            t_port = int(t_md.get('port') or input('port({}): '.format('27017')))
            t_user = t_md.get('user') or input('user({}): '.format('root'))
            t_pwd = t_md.get('password') or input('password({}): '.format('123456'))
            t_source = t_md.get('source') or input('source({}): '.format('admin'))
            self.todb_client = pymongo.MongoClient(self._format_uri(t_host, t_port, t_user, t_pwd, t_source))
            t_db = t_md.get('db') or self._show_dbs(db=self.todb_client)
            self.todb = self.todb_client[t_db]
            t_col = self._set_to_col(t_md.get('to_collection') or self.fromdb_collection)
            self.todb_collection = t_col
            fil = t_md.get('filter')
        else:
            t_host = input('host({}): '.format('127.0.0.1')) or '127.0.0.1'
            t_port = int(input('port({}): '.format('27017')) or '27017')
            t_user = input('user({}): '.format('root')) or 'root'
            t_pwd = input('password({}): '.format('123456')) or '123456'
            t_source = input('source({}): '.format('admin')) or 'admin'
            self.todb_client = pymongo.MongoClient(self._format_uri(t_host, t_port, t_user, t_pwd, t_source))
            t_db = self._show_dbs(db=self.todb_client)
            self.todb = self.todb_client[t_db]
            t_col = self._set_to_col(self.fromdb_collection)
            self.todb_collection = t_col
            fil = {}

        self.todb_set = self.todb[self.todb_collection]
        dic = {'host': t_host, 'port': t_port, 'user': t_user, 'password': t_pwd, 'source': t_source, 'db': t_db, 'to_collection': t_col, 'filter': fil}
        return dic

    def _save_mos(self, f_dic, t_dic):
        dic = {
            'from': f_dic,
            'to': t_dic
        }
        if not os.path.exists(self.default_mos_path):
            with open(self.default_mos_path, 'w+') as wf:
                wf.write(json.dumps(dic))

    def _show_dbs(self, db):
        sel = input('db(empty to show all): ')
        if sel:
            return sel
        db_names = db.list_database_names()
        print("------------ database names: ------------")
        for i, name in enumerate(db_names):
            print("[ {} ]: {}".format(i, name))
        print('------------------------------------------')
        sel = input("chose the num of the database's name: ")
        if sel:
            return db_names[int(sel)]
        else:
            sys.exit(0)

    def _show_clos(self, db):
        sel = input('collection(empty to show all): ')
        if sel:
            return sel

        names = db.list_collection_names(include_system_collections=False)
        print("------------ collection names: ------------")
        for i, name in enumerate(names):
            print("[ {} ]: {}".format(i, name))
        print('------------------------------------------')
        sel = input("chose the num of the collection's name: ")
        sel = re.findall(r'\d+', sel)
        sel = int(sel[0]) if sel else None
        if sel not in [x for x in range(len(names))]:
            raise ValueError('wrong input ! ')
        return names[sel]

    def _set_to_col(self, col=None):
        cols_in_db = self._get_to_clos()
        if col and col != self.fromdb_collection:
            if col in cols_in_db:
                pass
            else:
                self.todb.create_collection(col)
        else:
            col_def = self.fromdb_collection + '_copy'
            col = input('copy to collection({}): '.format(col_def)) or col_def
        return col

    def _get_to_clos(self):
        names = self.todb.list_collection_names(include_system_collections=True)
        return set(names)

    def _get_filer(self, db_set, collection):
        sel = input('input condition dict(empty to show all keys, or input "a" to copy all data): ')
        if sel.lower() == 'a':
            return {}
        if not sel:
            doc = db_set.find_one()
            doc = dict(doc) if doc else dict()
            keys_lis = self._get_key_path(doc)
            print('------------ keys names in [ {} ]: ------------'.format(collection))
            for i, name in enumerate(keys_lis):
                print("[ {} ]: {}".format(i, name))
            print('--------------------------------------------------------')
            sel = input("input condition dict(such as {'%s': 'condition_value'}), input 'a' to copy all data: " % keys_lis[-1])
            if sel == 'a':
                return {}
        try:
            sel = json.loads(sel)
        except:
            print('wrong input, make sure it is a json format')
            time.sleep(1)
            return self._get_filer(db_set, collection)
        return sel

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

    def _get_check_keys(self, mos):
        check_keys = mos.get('check_keys')
        if check_keys:
            return check_keys
        doc = self.todb_set.find_one()
        doc = dict(doc) if doc else dict()
        keys_lis = self._get_key_path(doc)
        print('------------ keys names in [ {} ]: ------------'.format(self.todb_collection))
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
                return self._get_check_keys(mos)
            else:
                sys.exit(1)

    @staticmethod
    def _format_uri(host, port, user, pwd, sou):
        uri = 'mongodb://{}:{}@{}:{}/{}'.format(user, pwd, host, port, sou)
        return uri

    @staticmethod
    def _mos_path(args, default=None):
        args = args or default
        return args

    def __del__(self):
        self.fromdb_client.close()
        self.todb_client.close()
        print('system exits')


def dl_starter(args=None):
    args = args if args else (sys.argv[1] if len(sys.argv) > 1 else None)
    ck = MongodbDuplicateChecker(args)
    ck.start()


def cp_starter(args=None):
    args = args if args else (sys.argv[1] if len(sys.argv) > 1 else None)
    mc = MongodbCopy(args)
    mc.start_copy()


if __name__ == '__main__':
    # arg = sys.argv[1] if len(sys.argv) > 1 else None
    # starter(arg)

    mcy = MongodbCopy()
    mcy.start_copy()
