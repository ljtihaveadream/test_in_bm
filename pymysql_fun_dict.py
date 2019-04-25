
import configparser
import datetime
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
            result = self.cursor.fetchone()

        except Exception as e:
            print(e)
        return result
    #查询所有的方法
    def selects(self,sql_list,log_msg=''):
        count = 0
        result = []
        for i,sql in enumerate(sql_list, 1):
            try:
                self.connect()
                count = self.cursor.execute(sql)
                data = self.cursor.fetchall()
                # print(data ,sql)
                # self.conn.commit()
                result.append({f'data_select_sql_{i}':data})
            except Exception as e:
                self.conn.rollback()
                print(e)
            finally:
                self.close()
                log_info = '查询条数' + str(count) + '条 ' + log_msg
                log(log_info)

        return result

    # 'MysqlHelper' object has no attribute '__edit'  私有方法
    # def __edit(self,sql,params=[],log_msg=''):#'MysqlHelper' object has no attribute '__edit'
    def edit(self,sql,log_msg=''):#'MysqlHelper' object has no attribute '__edit'
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
            log_info = '受影响条数' + str(count) + '条 '+log_msg
            log(log_info)
            return {'data':data,"count":count}

    def edits(self,sql_list,params=[],log_msg=''):
        """create_sql返回为空，默认为0"""
        count=0
        result=[]
        for sql in sql_list:
            try:
                self.connect()
                count=self.cursor.execute(sql,params)
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
    def update(self, sql):
        return self.edit(sql)

    # 删除的方法
    def delete(self, sql, params=[]):
        return self.edit(sql, params)

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

        except Exception as e:
            self.conn.rollback()
            return print(e)
        finally:
            self.close()
            log_info = log_msg + str(count) + '条'
            log(log_info)
            return {'count': count, 'last_key_id': last_key_id,}

    # 增加的方法
    def inserts(self, sql, data_listtuple,log_msg='成功插入数据'):
        """批量添加数据，数据格式必须list[tuple(),tuple(),tuple()]  或者tuple(tuple(),tuple(),tuple())
        """
        count = 0
        last_key_id = None
        try:
            self.connect()
            count = self.cursor.executemany(sql, data_listtuple)
            last_key_id = self.cursor.lastrowid
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            return print(e)
        finally:
            self.close()
            log_info=log_msg+str(count)+'条'
            log(log_info)
            return {'count': count, 'last_key_id': last_key_id,}

    #md5加密的方法
    # def my_md5(self, pwd):
    #     my_md5 = hashlib.md5()
    #     my_md5.update(pwd.encode('utf-8'))
    #     return my_md5.hexdigest()



if __name__ == '__main__':
    DB_CONN_CONFIG_TO={}
    cf = configparser.ConfigParser()
    cf.read("insert_config.ini", encoding="utf-8-sig")

    # global DB_CONN_CONFIG_TO  host,database, user, password
    DB_CONN_CONFIG_TO["host"] = cf.get("to_bd_info", "host")
    DB_CONN_CONFIG_TO["port"] = cf.getint("to_bd_info", "port")
    DB_CONN_CONFIG_TO["user"] = cf.get("to_bd_info", "user")
    DB_CONN_CONFIG_TO["password"] = cf.get("to_bd_info", "password")
    DB_CONN_CONFIG_TO["database"] = cf.get("to_bd_info", "database")
    mysql=MysqlHelper(**DB_CONN_CONFIG_TO)
    select_1="select * from job_trigger_info t where t.id=122;"
    # print(mysql.select_one(select_1))
    insert_1="""insert into name_age(name,age) values('b',2),('c',3)"""
    # print(mysql.insert(insert_1))
    insert_all = """insert into name_age(name,age) values(%s,%s)"""
    data=[('b',2)]
    print(mysql.insert_all(insert_all,data))
