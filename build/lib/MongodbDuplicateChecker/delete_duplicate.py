import json
import os
import re
import sys
import time

import pymongo
from pymongo.errors import OperationFailure
from tqdm import tqdm

from MongodbDuplicateChecker.gears import printer


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
        printer('system start', fill_with='=', alignment='m')
        printer(f'processing target [ {self.collection} ]')
        db_data = self.db_set.find({}, {x: 1 for x in self.check_keys}, no_cursor_timeout=True, batch_size=1000)
        self._process(db_data=db_data)
        printer(f'[ {self.collection} ] duplicate check done')

    def _process(self, db_data):
        warn = "Data is INVALUABLE! " \
               "Please make sure you are fully understanding what you are doing in the following steps!"
        printer(warn, fill_with='$', msg_head_tail=['\n', '\n'])
        time.sleep(3)
        counter = set()
        del_set = set()
        del_success = 0
        total = 0
        printer('start checking')
        t = tqdm(total=db_data.count())
        for data in db_data:
            d_lis = []
            for x in self.check_keys:
                value = eval('data["' + '"]["'.join(x.split('.')) + '"]')
                d_lis.append(str(value))
            d_str = '-'.join(d_lis)
            if d_str not in counter:
                counter.add(d_str)
            else:
                del_set.add(data.get("_id"))
            t.update()
            total += 1
        t.close()
        printer('check done')
        duplicate_count = len(del_set)
        if duplicate_count > 0:
            printer(f"done! total: [ {total} ], duplicate data: [ {duplicate_count} ]")
            del_sta = input('Do you warn to delete them all? (y/n): ').lower()
            if del_sta == 'y':
                tq = tqdm(total=duplicate_count)
                for dt in del_set:
                    try:
                        self.db_set.delete_one({"_id": dt})
                        del_success += 1
                    except Exception as E:
                        printer(f"delete err! {E}")
                        continue
                    tq.update()
                tq.close()
                printer(f'delete success: [ {del_success} ]')
        else:
            printer(f'total: [ {total} ], no duplicate data found')

    def _get_check_keys(self, mos):
        check_keys = mos.get('check_keys')
        if check_keys:
            return check_keys
        doc = self.db_set.find_one()
        doc = dict(doc) if doc else dict()
        keys_lis = self._get_key_path(doc)
        printer(f'keys in [ {self.collection} ]', fill_with='*', alignment='m')
        for i, name in enumerate(keys_lis):
            printer(f"[ {i} ]: {name}")
        printer('', fill_with='*')
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
                os._exit(0)

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
        printer('collection names:', fill_with='*', alignment='m', msg_head_tail=['*', '*'])
        for i, name in enumerate(names):
            printer(f"[ {i} ]: {name}")
        printer('', fill_with='*')
        sel = input("chose the num of the collection's name to process: ")
        sel = re.findall(r'\d+', sel)
        sel = int(sel[0]) if sel else None
        if sel not in [x for x in range(len(names))]:
            raise KeyboardInterrupt('wrong input ! ')
        return names[sel]

    def _db_name(self, mos):
        names = mos.get('db')
        if names:
            return names
        try:
            names = self.client.list_database_names()
            self._save_into_file(mos)
        except OperationFailure:
            printer('the mongodb setting maybe wrong! please check it and restart')
            for k, v in mos.items():
                printer(f"{k}: {v}")
            sys.exit(1)

        printer("database names:", fill_with='*', alignment='m', msg_head_tail=['*', '*'])
        for i, name in enumerate(names):
            printer(f"[ {i} ]: {name}")
        printer('', fill_with='*')
        sel = input("chose the num of the database's name to process: ")
        sel = re.findall(r'\d+', sel)
        sel = int(sel[0]) if sel else None
        if sel not in [x for x in range(len(names))]:
            raise KeyboardInterrupt('wrong input ! ')
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
            printer(f'servers found [ {mos_file} ]:', fill_with='*', alignment='m')
            for i, dic in enumerate(mos_lis_raw):
                mos_info = "{}:{}, {}, {}".format(dic.get('host'), dic.get('port'), dic.get('name'), dic.get('source'))
                printer(f"[ {i} ]: {mos_info}")
            printer('', fill_with='*')
            selection = input('chose the num of the server: ')
            selection = re.findall(r'\d+', selection)
            selection = int(selection[0]) if selection else None
            if selection not in [x for x in range(len(mos_lis_raw))]:
                raise KeyboardInterrupt('wrong input! ')
            mos_temp = mos_lis_raw[selection]
            return mos_temp
        else:
            printer('mongodb_servers file not exits! ')
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
                raise KeyboardInterrupt('wrong input! ')

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
        printer('system exit', fill_with='=')
        self.client.close()


class MongodbCopy(object):
    def __init__(self, args=None):
        self.default_mos_path = self._mos_path(args, '')
        self.save_mos_filename = 'mongocopy'
        self.fromdb_collection = None
        self.fromdb_client = None
        self.fromdb = None
        self.fromdb_set = None
        self.f_db_signature = None
        self.todb_collection = None
        self.todb_client = None
        self.todb = None
        self.todb_set = None
        self.condition = None
        self.t_db_signature = None
        self.tdb_str = None

    def start_copy(self):
        try:
            self.f_mos = self._get_from_mos()
            self.t_mos = self._get_to_mos()
            self.filter = self._get_filter()
            self._save_mos(self.f_mos, self.t_mos)
            printer('scanning documents ...')
            t_count = self.todb_set.count_documents({})
            f_count = self.fromdb_set.count_documents(self.condition)
            sk_str = f'skip documents(source db: {f_count}, target db: {t_count}, empty to set as: {t_count})'
            f_skip = input(sk_str) or str(t_count)
            f_skip = int(f_skip) if f_skip and isinstance(f_skip, str) and f_skip.isdigit() else 0
            to_data_set = set()
            self.t_db_signature += f".{self.todb_collection}"
            self.f_db_signature += f".{self.fromdb_collection}"
            cp_msg = f"copy start: from [ {self.f_db_signature} ] to [ {self.t_db_signature} ]"
            printer(cp_msg, alignment='m', msg_head_tail=['', ''], fill_with='*')
            if self.filter:
                to_data = self.todb_set.find({}, {x: 1 for x in self.filter},
                                             no_cursor_timeout=True, batch_size=1000)
                printer(f'preparing target [ {self.tdb_str} ] data')
                tt = tqdm(total=t_count)
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
                printer('target ready')
            from_data = self.fromdb_set.find(self.condition, {x: 1 for x in self.filter}, no_cursor_timeout=True, batch_size=500, skip=f_skip)
            printer('start copy ...')
            time.sleep(0.1)
            t = tqdm(total=f_count-f_skip)
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
            printer('process done')
            printer(f'copy done! total: [ {f_count-f_skip} ], success: [ {count} ], duplicate [ {dup_count} ]')
        except KeyboardInterrupt:
            self.end()

    def _get_filter(self):
        filter_ori = self.t_mos.get('filter')
        i_info = "input check keys(input 'i' to direct insert, empty to show all keys): "
        if filter_ori:
            i_info = f"input check keys({filter_ori}, input 'i' to direct insert, empty to show all keys): "
        sel = filter_ori or input(i_info)
        if sel.lower() == 'i':
            return None
        if sel:
            try:
                sel = json.loads(json.dumps(sel))
            except:
                printer('wrong input, make sure it is a json format')
                time.sleep(1)
                return self._get_filter()
        else:
            sel = self._get_check_keys(self.t_mos)
            if not sel:
                return self._get_filter()
            elif sel == 'no':
                printer(f'Collection [ {self.todb_collection} ] not exist, will create it')
                return None
        return sel

    def _get_from_mos(self):
        printer('copy data from:')
        mos_raw = {}
        if os.path.exists(self.default_mos_path):
            with open(self.default_mos_path, 'r') as rf:
                mos_raw = json.loads(rf.read())

        f_md = mos_raw.get('from') or {}
        f_host = f_md.get('host') or input(f'host(127.0.0.1): ') or "127.0.0.1"
        f_port = int(f_md.get('port') or input(f'port(27017): ') or "27017")
        f_user = f_md.get('user') or input(f'user(root): ') or "root"
        f_pwd = f_md.get('password') or input(f'password(123456): ') or "123456"
        f_source = f_md.get('source') or input(f'source(admin): ') or "admin"

        self.fromdb_client = pymongo.MongoClient(self._format_uri(f_host, f_port, f_user, f_pwd, f_source))
        f_db = self._show_dbs(db=self.fromdb_client)
        self.fromdb = self.fromdb_client[f_db]
        self.f_db_signature = f"{f_user}@{f_host}.{f_port}.{f_db}"
        f_col = self._show_clos(db=self.fromdb)
        self.fromdb_collection = f_col
        self.fromdb_set = self.fromdb[self.fromdb_collection]
        con = self._get_filer(self.fromdb_set, self.fromdb_collection)
        self.condition = con

        self.f_db_save_name = f"{f_host}-{f_db}"
        dic = {'host': f_host, 'port': f_port, 'user': f_user, 'password': f_pwd, 'source': f_source}
        return dic

    def _get_to_mos(self):
        printer('copy data to:')
        mos_raw = {}
        if os.path.exists(self.default_mos_path):
            with open(self.default_mos_path, 'r') as rf:
                mos_raw = json.loads(rf.read())
        t_md = mos_raw.get('to') or {}
        t_host = t_md.get('host') or input(f'host(127.0.0.1): ') or "127.0.0.1"
        t_port = int(t_md.get('port') or input(f'port(27017): ') or "27017")
        t_user = t_md.get('user') or input(f'user(root): ') or "root"
        t_pwd = t_md.get('password') or input(f'password(123456): ') or "123456"
        t_source = t_md.get('source') or input(f'source(admin): ') or "admin"

        self.todb_client = pymongo.MongoClient(self._format_uri(t_host, t_port, t_user, t_pwd, t_source))
        t_db = self._show_dbs(db=self.todb_client)
        self.todb = self.todb_client[t_db]
        self.t_db_signature = f"{t_user}@{t_host}.{t_port}.{t_db}"
        t_col = self._set_to_col()
        self.todb_collection = t_col
        self.tdb_str = f"{t_host}:{t_db}.{t_col}"
        self.t_db_save_name = f"{t_host}-{t_db}"
        self.todb_set = self.todb[self.todb_collection]
        dic = {'host': t_host, 'port': t_port, 'user': t_user, 'password': t_pwd, 'source': t_source}
        return dic

    def _save_mos(self, f_dic, t_dic):
        f_dic = self.pop_out(f_dic, ['db', 'from_collection', 'condition'])
        t_dic = self.pop_out(t_dic, ['db', 'to_collection', 'filter'])
        dic = {
            'from': f_dic,
            'to': t_dic
        }
        save_mos_path = self.save_mos_filename + f'-FROM_{self.f_db_save_name}_TO_{self.t_db_save_name}.json'
        with open(save_mos_path, 'w+') as wf:
            wf.write(json.dumps(dic))

    def pop_out(self, dic: dict, keys: list):
        for key in keys:
            if key in dic:
                dic.pop(key)
        return dic

    def _show_dbs(self, db):
        sel = input('db(empty to show all): ')
        if sel:
            return sel
        db_names = db.list_database_names()
        printer("database names:", fill_with='*', alignment='m', msg_head_tail=['*', '*'])
        for i, name in enumerate(db_names):
            printer(f"[ {i} ]: {name}")
        printer('*', fill_with='*')
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
        names.sort()
        printer("collection names:", fill_with='*', alignment='m', msg_head_tail=['*', '*'])
        for i, name in enumerate(names):
            printer(f"[ {i} ]: {name}")
        printer('*', fill_with='*')
        sel = input("chose the num of the collection's name: ")
        sel = re.findall(r'\d+', sel)
        sel = int(sel[0]) if sel else None
        if sel not in [x for x in range(len(names))]:
            raise KeyboardInterrupt('wrong input ! ')
        return names[sel]

    def _set_to_col(self):
        cols_in_db = self._get_to_clos()
        col_def = self.fromdb_collection + '_copy' if self.t_db_signature == self.f_db_signature else self.fromdb_collection
        col_def = col_def if col_def not in cols_in_db else col_def + '_copy'
        col = input(f'copy to collection({col_def}): ') or col_def
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
            printer(f'keys in [ {collection} ]:', fill_with='*', alignment='m', msg_head_tail=['*', '*'])
            for i, name in enumerate(keys_lis):
                printer(f"[ {i} ]: {name}")
            printer('*', fill_with='*')
            sel = input(
                "input condition dict(such as {'%s': 'condition_value'}), input 'a' to copy all data: " % keys_lis[-1])
            if sel == 'a':
                return {}
        try:
            sel = json.loads(sel)
        except:
            printer('wrong input, make sure it is a json format')
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
        if not keys_lis:
            return 'no'
        printer(f'keys in [ {self.todb_collection} ]:', fill_with='*', alignment='m', msg_head_tail=['*', '*'])
        for i, name in enumerate(keys_lis):
            printer(f"[ {i} ]: {name}")
        printer('*', fill_with='*')
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

    def end(self):
        try:
            self.fromdb_client.close()
            self.todb_client.close()
        except AttributeError:
            pass
        printer('system exits')


def dl_starter(args=None):
    args = args if args else (sys.argv[1] if len(sys.argv) > 1 else None)
    ck = MongodbDuplicateChecker(args)
    ck.start()


def cp_starter(args=None):
    args = args if args else (sys.argv[1] if len(sys.argv) > 1 else None)
    mc = MongodbCopy(args)
    mc.start_copy()


if __name__ == '__main__':
    # mcy = MongodbCopy()
    # mcy.start_copy()

    mck = MongodbDuplicateChecker(None)
    mck.start()

