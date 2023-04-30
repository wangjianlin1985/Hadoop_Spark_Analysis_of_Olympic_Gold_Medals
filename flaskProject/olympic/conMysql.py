import pymysql
from olympic import confInfo
from sqlalchemy import create_engine


class Connect:
    """
    本类提过了mysql的自定义api 可以过去mysql的连接 或者连接之后的 游标 执行sql 语句
    方便后续的mysql 连接对象的获取使用了单列设计模式让他全局只有一份 但是方法提供了单独的
    连接对象获取 如果你需要额外的获取连接对象可以调用getMysqlConnect 他将会返回一个mysqlConnect对象给你
    """
    conf = None
    puse = None
    initFlag = False
    __pyengine = None

    def __new__(cls):
        if cls.puse is None:
            cls.puse = super().__new__(cls)
        return cls.puse

    def __init__(self):
        if self.__pyengine is None:
            self.__pyengine = self.__selfConnect()
            if self.initFlag:
                return
            self.initFlag = True

    def __selfConnect(self):

        pyengine = None

        try:
            pyengine = pymysql.connect(host=confInfo.IP,
                                       user=confInfo.MYSQL_USER,
                                       password=confInfo.MYSQL_PASSWORD,
                                       database=confInfo.MYSQL_DATABASE)
            print(confInfo.IP + '=======>' + "连接成功")
        except pymysql.Error as e:
            print(e)
        return pyengine

    def getMysqlConnect(self):
        return self.__selfConnect()

    # 返回整个连接不建议次操作 如果有一个因为close 方法暴露
    def getAccept(self):
        return self.__pyengine

    # 执行一个查询语句
    def selectSql(self, sql):
        cursor = self.__pyengine.cursor()
        cursor.execute(sql)
        result = list()
        for i in cursor.fetchall():
            result.append(i[0])
        cursor.close()
        return result

    # 执行一个插入语句
    def insertSql(self, sql):
        cursor = self.__pyengine.cursor()
        try:
            cursor.execute(sql)
        except pymysql.Error as e:
            print(e)
            return False
        else:
            cursor.close()
        return True

    # 执行一个删除语句
    def dorpSql(self, sql):
        return self.insertSql(sql)

    # 获取一个游标
    def getCursor(self):
        return self.__pyengine.cursor()


# 此方法可以返回供 pandas使用的create_engine
def get_createengine():

    return create_engine(
        'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(confInfo.MYSQL_USER,
                                                             confInfo.MYSQL_PASSWORD,
                                                             confInfo.IP,
                                                             confInfo.MYSQL_PORT,
                                                             confInfo.MYSQL_DATABASE))
