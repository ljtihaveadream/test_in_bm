# 数据库取出的时间类型需要处理(insert时)，str() /datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
import configparser
import datetime,sys
try:
    import pymysql
except:
    import MySQLdb as pymysql


# import hashlib
def log(*msg):
    print(now(), msg)

def now():
    dt=datetime.datetime.now()
    return datetime.datetime.strftime(dt,'%Y-%m-%d %H:%M:%S')

#创建一个类
class MysqlHelper():
    #初始化属性
    def __init__(self, host,database, user, password,port=3306,charset='utf8'):
        self.host=host
        self.port=port
        self.db=database
        self.user=user
        self.passwd=password
        self.charset=charset
        self.cursorclass = pymysql.cursors.DictCursor

    #链接的方法
    def connect(self):
        self.conn=pymysql.connect(host=self.host, port=self.port, db=self.db, user=self.user, password=self.passwd, charset=self.charset,cursorclass = self.cursorclass)
        self.cursor=self.conn.cursor()
    #关闭的方法
    def close(self):
        self.cursor.close()
        self.conn.close()
    #查询一个的方法 返回一条数据 字典形式
    def select(self,sql):
        result=None
        try:
            self.connect()
            self.cursor.execute(sql)
            result = self.cursor.fetchall()

        except Exception as e:
            print(e)
        return result
    #查询所有的方法
    def selects(self,sql_list,log_msg=''):
        if type(sql_list) is list:
            count = 0
            result = {}
            for i, sql in enumerate(sql_list, 1):
                # for sql in sql_list:
                try:
                    self.connect()
                    count = self.cursor.execute(sql)
                    data = self.cursor.fetchall()
                    # print(data ,sql)
                    # self.conn.commit()
                    # result.append({f'data_select_sql_{i}':data})
                    result[f'sql{i}'] = data
                except Exception as e:
                    self.conn.rollback()
                    print(e)
                finally:
                    self.close()
                    log_info = f'select_sql_{i}查询结果：' + str(count) + '条' + log_msg
                    log(log_info)

            return result
        else:
            print('参数格式错误，该函数需要sql_list')

    # 'MysqlHelper' object has no attribute '__edit'  私有方法
    # def __edit(self,sql,params=[],log_msg=''):#'MysqlHelper' object has no attribute '__edit'
    def __edit(self,sql,log_msg=''):#'MysqlHelper' object has no attribute '__edit'
        """没有结果 返回空元组"""
        count=0
        data=None
        try:
            self.connect()
            count=self.cursor.execute(sql)
            data = self.cursor.fetchall()
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            return print(e)
        finally:
            self.close()
            log_info = log_msg+' 受影响条数 ' + str(count) + '条 '
            log(log_info)
            return {'data':data,"count":count}

    def __edits(self,sql_list,log_msg=''):
        """create_sql返回为空，默认为0"""
        count=0
        result=[]
        for sql in sql_list:
            try:
                self.connect()
                count=self.cursor.execute(sql)
                data=self.cursor.fetchall()
                # print(data ,sql)
                self.conn.commit()
                result.append(data if data or len(data)!=0 else (0,))
            except Exception as e:
                self.conn.rollback()
                print(e)
            finally:
                self.close()
                log_info = '受影响条数' + str(count) + '条 '+log_msg
                log(log_info)

        return result

    # 修改的方法
    def update(self, sql,log_msg='更新数据'):
        return self.__edit(sql,log_msg)

    # 删除的方法
    def delete(self, sql, log_msg='删除数据'):
        return self.__edit(sql,log_msg)

    #增加的方法
    def insert(self, sql,log_msg='成功插入数据'):
        """最新一条记录，就是刚刚插入的记录的第一条
            插入单条，参数为sql要替代的参数
        """
        count = 0
        last_key_id=None
        try:
            self.connect()
            count = self.cursor.execute(sql)
            last_key_id=self.cursor.lastrowid
            self.conn.commit()
            log_info = log_msg + str(count) + '条'
            log(log_info)
        except Exception as e:
            self.conn.rollback()
            return print(e)
        finally:
            self.close()
            return {'count': count, 'last_key_id': last_key_id,}

    # 增加的方法
    def inserts(self,table,list_dict,log_msg='成功插入数据'):
        """批量添加数据，数据格式必须list[tuple(),tuple(),tuple()]  或者tuple(tuple(),tuple(),tuple())
        """
        try:
            kv_data=self.get_kv_data_for_inserts(list_dict)
            print('字典列表数据解析为kv_data：',kv_data)
        except Exception as e:
            print('请传入正确参数')
            print(e)
            exit()
        # insert_= f"""insert into datagov.t_etl_io_original ({result_kv_data_io['key']}) values ({result_kv_data_io['value']});"""
        sql=f"""insert into {table} ({kv_data['key']}) values ({kv_data['value']});"""
        count = 0
        last_key_id = None
        try:
            self.connect()
            print('insert_sql',sql)
            count = self.cursor.executemany(sql, kv_data['data'])
            last_key_id = self.cursor.lastrowid
            self.conn.commit()
            log_info=log_msg+f'，目标表：{table}，条数：{count}，起始id：{last_key_id}'
            log(log_info)
        except Exception as e:
            self.conn.rollback()
            return print(e)
        finally:
            self.close()
            return {'table':table,'count': count, 'last_key_id': last_key_id }

    def get_kv_data_for_inserts(self,list_dict):
        """
        多条数据对象调用，返回值的3 个对象都用。同样适用单条数据调用，传参为 [dict]
        sql=f"insert into table ({return['key']}) values ({return['value']});"
        result_sql=mysql.inserts(sql,return['data'])
        :param list_dict: [dict1,dict2...]
        :return: {'key':key_str,'value':value_str,'data':data_values}
        """
        try:
            key_str = ','.join(list(list_dict[0].keys()))
            value_str_list=[]
            for i in range(len(list(list_dict[0].keys()))):
                value_str_list.append('%s')
            value_str=','.join(value_str_list)
            data_values=[] # 说list包含数字，不能直接转化成字符串。
            for dict in list_dict:#每一条数据
                value_list = list(dict.values())
                v_list = []
                for v in value_list:#吧每一个value变成str
                    v_str = str(v)
                    v_list.append(v_str)
                data_values.append(tuple(v_list)) # 说list包含数字，不能直接转化成字符串。
            return {'key':key_str,'value':value_str,'data':data_values}
        except Exception as e:
            print('请传入正确参数。eg:[{key1:value11,key2:value12...},{key1:value21,key2:value22...}....]')

    #md5加密的方法
    # def my_md5(self, pwd):
    #     my_md5 = hashlib.md5()
    #     my_md5.update(pwd.encode('utf-8'))
    #     return my_md5.hexdigest()


    #该函数的作用是执行一些不关注返回结果的语句
    def exec(self, sql_list, params=[]):
        '''
        该函数的作用是执行一些不关注返回结果的语句
        :param sql:
        :param params: 其他参数。一般为空
        :return: bool值，表成功失败
        '''
        b_result = False
        for i,sql in enumerate(sql_list,1):#同时获得索引和值,指定index从1开始
            try:
                self.connect()
                count = self.cursor.execute(sql, params)
                self.conn.commit()
                b_result = True
            except Exception as e:
                self.conn.rollback()
                return log(e)
            finally:
                self.close()
                print(f'第{i}条sql执行结果：{b_result}')
                return b_result


if __name__ == '__main__':
    DB_CONN_CONFIG_TO={}

    DB_CONN_CONFIG_TO["host"] = 'localhost'
    DB_CONN_CONFIG_TO["port"] = 3306
    DB_CONN_CONFIG_TO["user"] = 'root'
    DB_CONN_CONFIG_TO["password"] = 'root'
    DB_CONN_CONFIG_TO["database"] = 'datagov'
    mysql=MysqlHelper(**DB_CONN_CONFIG_TO)
    select_1="select * from job_trigger_info t where t.id=122;"
    # print(mysql.select_one(select_1))
    insert_1="""insert into name_age(name,age) values('b',2),('c',3)"""
    # print(mysql.insert(insert_1))
    insert_all = """insert into name_age(name,age) values(%s,%s)"""
    data=[('b',2)]
    # print(mysql.insert_all(insert_all,data))
    sql_delete="delete from system_source_meta where system_name='Hive库' and DATE_FORMAT(create_time,'%Y-%m-%d')='2019-03-11';"
    # print(mysql.delete(sql_delete))
